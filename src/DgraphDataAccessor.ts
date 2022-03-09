import type { Readable } from 'stream';
import {
  RepresentationMetadata,
  NotFoundHttpError,
  NotImplementedHttpError,
  UnsupportedMediaTypeHttpError,
  getLoggerFor,
  isContainerIdentifier,
  guardedStreamFrom,
  INTERNAL_QUADS,
  CONTENT_TYPE,
} from '@solid/community-server';
import type {
  Representation,
  ResourceIdentifier,
  IdentifierStrategy,
  DataAccessor,
  Guarded,
} from '@solid/community-server';
import arrayifyStream from 'arrayify-stream';
import dgraph from 'dgraph-js';
import { DataFactory } from 'n3';
import type { Quad, Term } from 'rdf-js';
import * as RdfString from 'rdf-string-ttl';
import { NON_RDF_KEYS, MAX_REQUEST_RETRIES, getGlobalDgraphClientInstance } from './DgraphUtil';

const { defaultGraph, namedNode } = DataFactory;

export interface DgraphQuery {
  queryString: string;
  vars: any;
}

/**
 * Stores all data and metadata of resources in a DGraph Database.
 */
export class DgraphDataAccessor implements DataAccessor {
  protected readonly logger = getLoggerFor(this);

  private readonly identifierStrategy: IdentifierStrategy;

  public constructor(identifierStrategy: IdentifierStrategy) {
    this.identifierStrategy = identifierStrategy;
  }

  /**
  * Only Quad data streams are supported.
  */
  public async canHandle(representation: Representation): Promise<void> {
    if (representation.binary || representation.metadata.contentType !== INTERNAL_QUADS) {
      throw new UnsupportedMediaTypeHttpError('Only Quad data is supported.');
    }
  }

  /**
  * Returns a data stream stored for the given identifier.
  * It can be assumed that the incoming identifier will always correspond to a document.
  * @param identifier - Identifier for which the data is requested.
  */
  public async getData(identifier: ResourceIdentifier): Promise<Guarded<Readable>> {
    const responseJSON = await this.sendDgraphQuery({
      queryString: `
        query data($identifier: string) {
          entity as var(func: eq(uri, $identifier)) @filter(eq(dgraph.type, ["Entity", "MetadataEntity"]))
          data(func: has(container)) @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "EntityData")) {
             expand(_userpredicate_)
          }
        }`,
      vars: { $identifier: RdfString.termToString(namedNode(identifier.path)) },
    });
    const quads = this.rdfQuadsFromJsonArray(responseJSON.data);
    return guardedStreamFrom(quads);
  }

  /**
  * Returns the metadata corresponding to the identifier.
  * @param identifier - Identifier for which the metadata is requested.
  */
  public async getMetadata(identifier: ResourceIdentifier): Promise<RepresentationMetadata> {
    const responseJSON = await this.sendDgraphQuery({
      queryString: `
        query data($identifier: string) {
          entity as var(func: eq(uri, $identifier)) @filter(eq(dgraph.type, ["Entity", "MetadataEntity"]))
          data(func: has(container)) @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "Metadata")) {
             expand(_userpredicate_)
          }
        }`,
      vars: { $identifier: RdfString.termToString(namedNode(identifier.path)) },
    });
    const quads = this.rdfQuadsFromJsonArray(responseJSON.data);

    if (quads.length === 0) {
      throw new NotFoundHttpError();
    }

    const metadata = new RepresentationMetadata(identifier).addQuads(quads);
    if (!isContainerIdentifier(identifier)) {
      metadata.contentType = INTERNAL_QUADS;
    }

    return metadata;
  }

  /**
  * Returns metadata for all resources in the requested container.
  * This should not be all metadata of those resources (but it can be),
  * but instead the main metadata you want to show in situations
  * where all these resources are presented simultaneously.
  * Generally this would be metadata that is present for all of these resources,
  * such as resource type or last modified date.
  *
  * It can be safely assumed that the incoming identifier will always correspond to a container.
  *
  * @param identifier - Identifier of the parent container.
  */
  public async* getChildren(identifier: ResourceIdentifier): AsyncIterableIterator<RepresentationMetadata> {
    const responseJSON = await this.sendDgraphQuery({
      queryString: `
        query data($identifier: string) {
          entity as var(func: eq(uri, $identifier)) @filter(eq(<dgraph.type>, "Entity"))
          data(func: eq(<dgraph.type>, "Entity")) @filter(uid_in(<container>, uid(entity))) {
            uri
          }
        }`,
      vars: { $identifier: RdfString.termToString(namedNode(identifier.path)) },
    });
    for (const result of responseJSON.data) {
      yield new RepresentationMetadata(namedNode(result.uri));
    }
  }

  /**
  * Writes data and metadata for a document.
  * If any data and/or metadata exist for the given identifier, it should be overwritten.
  * @param identifier - Identifier of the resource.
  * @param data - Data to store.
  * @param metadata - Metadata to store.
  */
  public async writeDocument(identifier: ResourceIdentifier, data: Guarded<Readable>,
    metadata: RepresentationMetadata): Promise<void> {
    const { name, parent } = this.getRelatedNames(identifier);

    const triples = await arrayifyStream(data) as Quad[];
    const def = defaultGraph();
    if (triples.some((triple): boolean => !def.equals(triple.graph))) {
      throw new NotImplementedHttpError('Only triples in the default graph are supported.');
    }

    // Not relevant since all content is triples
    metadata.removeAll(CONTENT_TYPE);

    return await this.sendDgraphUpsert(name, metadata, parent, triples);
  }

  /**
  * Writes metadata for a container.
  * If the container does not exist yet it should be created,
  * if it does its metadata should be overwritten, except for the containment triples.
  * @param identifier - Identifier of the container.
  * @param metadata - Metadata to store.
  */
  public async writeContainer(identifier: ResourceIdentifier, metadata: RepresentationMetadata): Promise<void> {
    const { name, parent } = this.getRelatedNames(identifier);
    return await this.sendDgraphUpsert(name, metadata, parent);
  }

  /**
  * Deletes the resource and its corresponding metadata.
  *
  * Solid, §5.4: "When a contained resource is deleted, the server MUST also remove the corresponding containment
  * triple, which has the effect of removing the deleted resource from the containing container."
  * https://solid.github.io/specification/protocol#deleting-resources
  *
  * @param identifier - Resource to delete.
  */
  public async deleteResource(identifier: ResourceIdentifier): Promise<void> {
    const { name, parent } = this.getRelatedNames(identifier);
    return await this.sendDgraphDelete(name, parent);
  }

  private async sendDgraphQuery(query: DgraphQuery, tries = 1): Promise<any> {
    const txn = getGlobalDgraphClientInstance().newTxn({ readOnly: true });
    try {
      const response = await txn.queryWithVars(query.queryString, query.vars);
      return response.getJson();
    } catch (error: unknown) {
      if (error === dgraph.ERR_ABORTED && tries < MAX_REQUEST_RETRIES) {
        return await this.sendDgraphQuery(query, tries + 1);
      }

      throw error;
    } finally {
      await txn.discard();
    }
  }

  private async sendDgraphDelete(name: string, parentName?: string): Promise<void> {
    let query = `
      query {
        entity as var(func: eq(uri, "${this.toRdfString(name)}"))
          @filter(eq(dgraph.type, "Entity"))`;

    if (parentName) {
      query += `
        entityParent as var(func: eq(uri, "${this.toRdfString(parentName)}"))
          @filter(eq(dgraph.type, "Entity"))`;
    }

    query += `
        entitiesInName as var(func: has(container))
          @filter(uid_in(container, uid(entity)))
      }`;

    // Delete all entities where metaname is the container
    const deleteMutation = new dgraph.Mutation();
    deleteMutation.setDelNquads(`
      uid(entity) * * .
      uid(entitiesInName) * * .
      ${parentName ? `uid(entity) <container> uid(entityParent) .` : ''}
    `);

    const request = new dgraph.Request();
    request.setQuery(query);
    request.setMutationsList([ deleteMutation ]);
    request.setCommitNow(true);
    await this.transactDgraphRequest(request);
  }

  /**
   * Creates an upsert query that overwrites the data and metadata of a resource.
   * If there are no triples we assume it's a container (so don't overwrite the main graph with containment triples).
   * @param name - Name of the resource to update.
   * @param metadata - New metadata of the resource.
   * @param parentName - Name of the parent to update the containment triples.
   * @param triples - New data of the resource.
   */
  private async sendDgraphUpsert(name: string, metadata: RepresentationMetadata,
    parentName?: string, triples?: Quad[]): Promise<void> {
    const mutations: dgraph.Mutation[] = [];

    let query = `
      query {
        entity as var(func: eq(uri, "${this.toRdfString(name)}"))
          @filter(eq(dgraph.type, "Entity"))`;

    if (parentName) {
      query += `
        entityParent as var(func: eq(uri, "${this.toRdfString(parentName)}"))
          @filter(eq(dgraph.type, "Entity"))`;
    }

    if (triples) {
      query += `
        entityDataInName as var(func: has(container))
          @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "EntityData"))`;
    }

    query += `
      entityMetaInName as var(func: has(container))
        @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "Metadata"))
    }`;

    // Delete all metadata
    const deleteMetaMutation = new dgraph.Mutation();
    deleteMetaMutation.setDelNquads('uid(entityMetaInName) * * .');
    mutations.push(deleteMetaMutation);

    const setNQuads: string[] = [];
    // Make sure the parent's uri is set
    if (parentName) {
      setNQuads.push(`uid(entityParent) <uri> "${this.toRdfString(parentName)}" .`);
      setNQuads.push(`uid(entityParent) <dgraph.type> "Entity" .`);
    }
    // Make sure this entity's uri is set
    setNQuads.push(`uid(entity) <uri> "${this.toRdfString(name)}" .`);
    setNQuads.push(`uid(entity) <dgraph.type> "Entity" .`);
    // Create entities from metadata where metaName is the container
    const metadataQuadsGroupedBySubject = this.groupQuadsBySubject(metadata.quads());
    Object.values(metadataQuadsGroupedBySubject)
      .forEach((quads, i): void => {
        setNQuads.push(`_:m${i} <uri> "${RdfString.termToString(quads[0].subject)}" .`);
        setNQuads.push(`_:m${i} <container> uid(entity) .`);
        setNQuads.push(`_:m${i} <dgraph.type> "Metadata" .`);
        quads.forEach((quad): void => {
          setNQuads.push(
            `_:m${i} ${RdfString.termToString(quad.predicate)} "${this.termToEscapedString(quad.object)}" .`,
          );
        });
      });

    // Set the parent of this entity
    if (parentName) {
      setNQuads.push(`uid(entity) <container> uid(entityParent) .`);
    }

    // Create entity from triples where name is the container
    if (triples) {
      // Delete all entities where name is the container
      const deleteMutation = new dgraph.Mutation();
      deleteMutation.setDelNquads('uid(entityDataInName) * * .');
      mutations.push(deleteMutation);

      const tripleQuadsGroupedBySubject = this.groupQuadsBySubject(triples);
      Object.values(tripleQuadsGroupedBySubject)
        .forEach((quads, i): void => {
          setNQuads.push(`_:n${i} <uri> "${RdfString.termToString(quads[0].subject)}" .`);
          setNQuads.push(`_:n${i} <container> uid(entity) .`);
          setNQuads.push(`_:n${i} <dgraph.type> "EntityData" .`);
          quads.forEach((quad): void => {
            setNQuads.push(
              `_:n${i} ${RdfString.termToString(quad.predicate)} "${this.termToEscapedString(quad.object)}" .`,
            );
          });
        });
    }

    const insertMutation = new dgraph.Mutation();
    insertMutation.setSetNquads(setNQuads.join('\n'));
    mutations.push(insertMutation);

    const request = new dgraph.Request();
    request.setQuery(query);
    request.setMutationsList(mutations);
    request.setCommitNow(true);
    await this.transactDgraphRequest(request);
  }

  private async transactDgraphRequest(request: dgraph.Request, tries = 1): Promise<void> {
    const txn = getGlobalDgraphClientInstance().newTxn();
    try {
      await txn.doRequest(request);
    } catch (error: unknown) {
      if (error === dgraph.ERR_ABORTED && tries < MAX_REQUEST_RETRIES) {
        await this.transactDgraphRequest(request, tries + 1);
      } else {
        throw error;
      }
    } finally {
      await txn.discard();
    }
  }

  // Note on external ids in dgraph
  // https://dgraph.io/docs/mutations/external-ids/
  // https://dgraph.io/docs/mutations/external-ids-upsert-block/
  private rdfQuadsFromJsonArray(json: any[], parentSubject?: string, parentPredicate?: string): Quad[] {
    return json.reduce((rdf, entity): Quad[] => {
      const entityIsObject = typeof entity === 'object';
      if (!entityIsObject && parentSubject && parentPredicate) {
        rdf.push(RdfString.stringQuadToQuad({
          subject: parentSubject,
          predicate: this.toRdfString(parentPredicate),
          object: entity,
          graph: '',
        }));
      } else if (entityIsObject && parentSubject && parentPredicate && entity.uri) {
        rdf.push(RdfString.stringQuadToQuad({
          subject: parentSubject,
          predicate: this.toRdfString(parentPredicate),
          object: entity.uri,
          graph: '',
        }));
      } else if (entityIsObject && entity.uri) {
        const kv = Object.entries(entity);
        kv.forEach(([ key, value ]: any[]): void => {
          if (Array.isArray(value) && key !== 'dgraph.type') {
            rdf = [ ...rdf, ...this.rdfQuadsFromJsonArray(value, entity.uri, key) ];
          } else if (typeof value === 'object') {
            rdf = [ ...rdf, ...this.rdfQuadsFromJsonArray([ value ], entity.uri, key) ];
          } else if (!NON_RDF_KEYS.has(key)) {
            rdf.push(RdfString.stringQuadToQuad({
              subject: entity.uri,
              predicate: this.toRdfString(key),
              object: value,
              graph: '',
            }));
          }
        });
      }
      return rdf;
    }, []);
  }

  /**
   * Helper function to get named nodes corresponding to the identifier and its parent container.
   * In case of a root container only the name will be returned.
   */
  private getRelatedNames(identifier: ResourceIdentifier): { name: string; parent?: string } {
    const name = identifier.path;

    // Root containers don't have a parent
    if (this.identifierStrategy.isRootContainer(identifier)) {
      return { name };
    }

    const parentIdentifier = this.identifierStrategy.getParentContainer(identifier);
    const parent = parentIdentifier.path;
    return { name, parent };
  }

  private groupQuadsBySubject(quads: Quad[]): Record<string, Quad[]> {
    return quads.reduce((obj: Record<string, Quad[]>, quad): Record<string, Quad[]> => {
      if (!obj[quad.subject.value]) {
        obj[quad.subject.value] = [];
      }
      obj[quad.subject.value].push(quad);
      return obj;
    }, {});
  }

  private toRdfString(identifier: string): string {
    return RdfString.termToString(namedNode(identifier));
  }

  private termToEscapedString(term: Term): string {
    return RdfString.termToString(term).replace(/"/gu, '\\"');
  }
}
