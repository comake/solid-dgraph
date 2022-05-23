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
import { DataFactory, Literal } from 'n3';
import type { Quad_Subject, Quad_Predicate } from 'n3';
import type { Quad, NamedNode, BlankNode } from 'rdf-js';
import { DgraphClient } from './DgraphClient';
import { DgraphUpsert } from './DgraphUpsert';
import { NON_RDF_KEYS, DEFAULT_SCHEMA,
  INITIALIZATION_CHECK_PERIOD, MAX_INITIALIZATION_TIMEOUT_DURATION,
  wait, literalDatatypeToPrimitivePredicate } from './DgraphUtil';
import type { ValuePredicate, DgraphConfiguration } from './DgraphUtil';

const { defaultGraph, namedNode, quad, blankNode } = DataFactory;

export interface DgraphQuery {
  queryString: string;
  vars: any;
}

export type LiteralNodeKey = ValuePredicate | 'datatype' | 'language';
export type DgraphLiteralNode = {
  [key in LiteralNodeKey]?: string;
};

export type DgraphNamedNode = {
  uid: string;
  uri: string;
  blankNode: string;
  container?: string;
  'dgraph.type'?: string;
  [k: string]: undefined | string | DgraphNode | DgraphNode[];
};

export interface DgraphConfigurationArgs {
  connectionUri?: string;
  grpcPort?: string;
  schema?: string;
}

export const defaultConfiguration = {
  connectionUri: 'localhost',
  grpcPort: '9080',
};

export type DgraphNode = DgraphLiteralNode | DgraphNamedNode;

/**
 * Stores all data and metadata of resources in a DGraph Database.
 */
export class DgraphDataAccessor implements DataAccessor {
  protected readonly logger = getLoggerFor(this);

  private clientInitialized = false;
  private initializingClient = false;
  private dgraphClient?: DgraphClient;
  private readonly configuration: DgraphConfiguration;
  private readonly identifierStrategy: IdentifierStrategy;

  public constructor(identifierStrategy: IdentifierStrategy, configuration: DgraphConfigurationArgs) {
    this.configuration = { ...defaultConfiguration, ...configuration };
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
    const responseJSON = await this.sendDgraphClientQuery({
      queryString: `
        query data($identifier: string) {
          entity as var(func: eq(<uri>, $identifier)) @filter(eq(<dgraph.type>, "Entity"))
          data(func: has(container)) @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "EntityData")) {
             expand(_userpredicate_) {
               expand(_userpredicate_)
             }
          }
        }`,
      vars: { $identifier: identifier.path },
    });
    const quads = this.rdfQuadsFromJsonArray(responseJSON.data);
    return guardedStreamFrom(quads);
  }

  /**
  * Returns the metadata corresponding to the identifier.
  * @param identifier - Identifier for which the metadata is requested.
  */
  public async getMetadata(identifier: ResourceIdentifier): Promise<RepresentationMetadata> {
    const responseJSON = await this.sendDgraphClientQuery({
      queryString: `
        query data($identifier: string) {
          entity as var(func: eq(<uri>, $identifier)) @filter(eq(<dgraph.type>, "Entity"))
          data(func: has(container)) @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "Metadata")) {
             expand(_userpredicate_) {
               expand(_userpredicate_)
             }
          }
        }`,
      vars: { $identifier: identifier.path },
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
    const responseJSON = await this.sendDgraphClientQuery({
      queryString: `
        query data($identifier: string) {
          entity as var(func: eq(<uri>, $identifier)) @filter(eq(<dgraph.type>, "Entity"))
          data(func: eq(<dgraph.type>, "Entity")) @filter(uid_in(<container>, uid(entity))) {
            uri
          }
        }`,
      vars: { $identifier: identifier.path },
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

    const triples = await arrayifyStream(data);
    const def = defaultGraph();

    if (triples.some((triple): boolean => !def.equals(triple.graph))) {
      throw new NotImplementedHttpError('Only triples in the default graph are supported.');
    }

    // Not relevant since all content is triples
    metadata.removeAll(namedNode(CONTENT_TYPE));

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

  /**
  * Creates an upsert query that deleted the data and metadata,
  * and containment triple of a resource.
  * @param name - URI of the resource to delete.
  * @param parent - URI of the resource's container.
  */
  private async sendDgraphDelete(name: string, parent?: string): Promise<void> {
    const dgraphUpsert = new DgraphUpsert();
    const entityUidName = 'entity';
    const parentUidName = 'parent';
    const dataInEntityUidName = 'dataInEntity';

    dgraphUpsert.addQuery(this.entityUidVarQuery(entityUidName, name));
    dgraphUpsert.addQuery(`${dataInEntityUidName} as var(func: has(container))
        @filter(uid_in(container, uid(${entityUidName})))`);
    dgraphUpsert.addDelNquad(`uid(${entityUidName}) * * .`);
    dgraphUpsert.addDelNquad(`uid(${dataInEntityUidName}) * * .`);
    if (parent) {
      dgraphUpsert.addQuery(this.entityUidVarQuery(parentUidName, parent));
      dgraphUpsert.addDelNquad(`uid(${entityUidName}) <container> uid(${parentUidName}) .`);
    }

    await this.sendDgraphClientUpsert(dgraphUpsert);
  }

  /**
  * Creates an upsert query that overwrites the data and metadata of a resource.
  * If there are no triples we assume it's a container (so don't overwrite the main graph with containment triples).
  * @param name - URI of the resource to update.
  * @param metadata - New metadata of the resource.
  * @param parent - Name of the parent to update the containment triples.
  * @param triples - New data of the resource.
  */
  private async sendDgraphUpsert(name: string, metadata: RepresentationMetadata,
    parent?: string, triples?: Quad[]): Promise<void> {
    const dgraphUpsert = new DgraphUpsert();
    const entityUidName = 'entity';
    dgraphUpsert.addQuery(this.entityUidVarQuery(entityUidName, name));
    // Set entity's uri
    dgraphUpsert.addSetNquad(`uid(${entityUidName}) <uri> "${name}" .`);
    dgraphUpsert.addSetNquad(`uid(${entityUidName}) <dgraph.type> "Entity" .`);
    // Replace entity metadata
    this.replaceDataOfTypeInUidName(dgraphUpsert, metadata.quads(), entityUidName, 'Metadata');

    if (parent) {
      // Set the parent of this entity
      const parentEntityUidName = 'parent';
      dgraphUpsert.addQuery(this.entityUidVarQuery(parentEntityUidName, parent));
      dgraphUpsert.addSetNquad(`uid(${parentEntityUidName}) <uri> "${parent}" .`);
      dgraphUpsert.addSetNquad(`uid(${parentEntityUidName}) <dgraph.type> "Entity" .`);
      dgraphUpsert.addSetNquad(`uid(${entityUidName}) <container> uid(${parentEntityUidName}) .`);
    }

    if (triples) {
      // Replace entity data
      this.replaceDataOfTypeInUidName(dgraphUpsert, triples, entityUidName, 'EntityData');
    }

    await this.sendDgraphClientUpsert(dgraphUpsert);
  }

  private replaceDataOfTypeInUidName(dgraphUpsert: DgraphUpsert, quads: Quad[],
    entityUidName: string, dgraphType: string): void {
    // Delete all old data in this entity
    dgraphUpsert.addQuery(this.dataOfTypeInEntityQuery(entityUidName, dgraphType));
    dgraphUpsert.addDelNquad(`uid(${entityUidName}${dgraphType}) * * .`);
    // Insert new data in this entity
    this.setNQuadsAndQueriesForQuadsBySubjectInContainer(dgraphUpsert, quads, entityUidName, dgraphType);
  }

  private setNQuadsAndQueriesForQuadsBySubjectInContainer(dgraphUpsert: DgraphUpsert, quads: Quad[],
    containerUidName: string, dgraphType: string): void {
    const metadataQuadsGroupedBySubject = this.groupQuadsBySubject(quads);
    Object.values(metadataQuadsGroupedBySubject)
      .forEach((quadsWithSameSubject, i): void => {
        const blankNodeName = `${dgraphType}${i}`;
        if (quadsWithSameSubject[0].subject.termType === 'BlankNode') {
          dgraphUpsert.addSetNquad(`_:${blankNodeName} <blankNode> "${quadsWithSameSubject[0].subject.value}" .`);
        } else {
          dgraphUpsert.addSetNquad(`_:${blankNodeName} <uri> "${quadsWithSameSubject[0].subject.value}" .`);
        }
        dgraphUpsert.addSetNquad(`_:${blankNodeName} <container> uid(${containerUidName}) .`);
        dgraphUpsert.addSetNquad(`_:${blankNodeName} <dgraph.type> "${dgraphType}" .`);
        quadsWithSameSubject.forEach((rdfQuad, j): void => {
          const uidVar = `${blankNodeName}${j}`;
          if (rdfQuad.object.termType === 'Literal') {
            const key = literalDatatypeToPrimitivePredicate(rdfQuad.object.datatype.value);
            // eslint-disable-next-line no-useless-escape
            const value = rdfQuad.object.value.replace(/"/gu, '\\\"');
            dgraphUpsert.addQuery(`${uidVar} as var(func: eq(<${key}>, "${value}"))
              @filter(eq(<language>, "${rdfQuad.object.language}") and
                eq(<datatype>, "${rdfQuad.object.datatype.value}"))`);
            dgraphUpsert.addSetNquad(`_:${blankNodeName} <${rdfQuad.predicate.value}> uid(${uidVar}) .`);
            dgraphUpsert.addSetNquad(`uid(${uidVar}) <${key}> "${value}" .`);
            dgraphUpsert.addSetNquad(`uid(${uidVar}) <language> "${rdfQuad.object.language}" .`);
            dgraphUpsert.addSetNquad(`uid(${uidVar}) <datatype> "${rdfQuad.object.datatype.value}" .`);
          } else if (rdfQuad.object.termType === 'NamedNode') {
            dgraphUpsert.addQuery(this.entityUidVarQuery(uidVar, rdfQuad.object.value));
            dgraphUpsert.addSetNquad(`_:${blankNodeName} <${rdfQuad.predicate.value}> uid(${uidVar}) .`);
            dgraphUpsert.addSetNquad(`uid(${uidVar}) <uri> "${rdfQuad.object.value}" .`);
            dgraphUpsert.addSetNquad(`uid(${uidVar}) <dgraph.type> "Entity" .`);
          } else if (rdfQuad.object.termType === 'BlankNode') {
            dgraphUpsert.addQuery(this.blankNodeUidVarQuery(uidVar, containerUidName, rdfQuad.object.value));
            dgraphUpsert.addSetNquad(`_:${blankNodeName} <${rdfQuad.predicate.value}> uid(${uidVar}) .`);
            dgraphUpsert.addSetNquad(`uid(${uidVar}) <blankNode> "${rdfQuad.object.value}" .`);
            dgraphUpsert.addSetNquad(`uid(${uidVar}) <container> uid(${containerUidName}) .`);
            dgraphUpsert.addSetNquad(`uid(${uidVar}) <dgraph.type> "EntityData" .`);
          }
        });
      });
  }

  private entityUidVarQuery(uidName: string, uri: string): string {
    return `${uidName} as var(func: eq(<uri>, "${uri}"))
        @filter(eq(<dgraph.type>, "Entity"))`;
  }

  private blankNodeUidVarQuery(uidName: string, containerUidName: string, blankNodeName: string): string {
    return `${uidName} as var(func: eq(<blankNode>, "${blankNodeName}"))
        @filter(uid_in(container, uid(${containerUidName})) and eq(<dgraph.type>, "EntityData"))`;
  }

  private dataOfTypeInEntityQuery(entityUidName: string, dgraphType: string): string {
    return `${entityUidName}${dgraphType} as var(func: has(container))
      @filter(uid_in(container, uid(${entityUidName})) and eq(<dgraph.type>, "${dgraphType}"))`;
  }

  private async sendDgraphClientUpsert(upsert: DgraphUpsert): Promise<void> {
    await this.ensureClientIsInitialized();
    await this.dgraphClient!.sendDgraphUpsert(upsert.queries, upsert.delNquads, upsert.setNquads);
  }

  private async sendDgraphClientQuery(query: DgraphQuery): Promise<any> {
    await this.ensureClientIsInitialized();
    return this.dgraphClient!.sendDgraphQuery(query.queryString, query.vars);
  }

  private async ensureClientIsInitialized(waitTime = 0): Promise<void> {
    if (!this.clientInitialized && !this.initializingClient) {
      await this.initializeClient();
    } else if (!this.clientInitialized && this.initializingClient &&
      waitTime <= MAX_INITIALIZATION_TIMEOUT_DURATION) {
      await wait(INITIALIZATION_CHECK_PERIOD);
      await this.ensureClientIsInitialized(waitTime + INITIALIZATION_CHECK_PERIOD);
    } else if (!this.clientInitialized) {
      throw new Error('Failed to initialize Dgraph database.');
    }
  }

  private async initializeClient(): Promise<void> {
    this.initializingClient = true;
    this.logger.info(`Initializing Dgraph Client`);
    this.dgraphClient = new DgraphClient(this.configuration);
    try {
      await this.dgraphClient.setSchema(this.configuration.schema ?? DEFAULT_SCHEMA);
    } catch (error: unknown) {
      this.initializingClient = false;
      throw error;
    }
    this.clientInitialized = true;
    this.initializingClient = false;
    this.logger.info(`Initialized Dgraph Client`);
  }

  private rdfQuadsFromJsonArray(json: DgraphNode[], parent?: Quad_Subject, predicate?: Quad_Predicate): Quad[] {
    return json.reduce((rdf: Quad[], entity: DgraphNode): Quad[] => {
      if (parent && predicate) {
        rdf.push(quad(parent, predicate, this.dgraphNodeToTerm(entity)));
      } else if ('uri' in entity || 'blankNode' in entity) {
        const isBlankNode = 'blankNode' in entity && Boolean(entity.blankNode);
        const term = isBlankNode ? blankNode(entity.blankNode) : namedNode(entity.uri);
        Object.keys(entity)
          .filter((key): boolean => !NON_RDF_KEYS.has(key))
          .forEach((rdfPredicate): void => {
            const predicateTerm = namedNode(rdfPredicate);
            const object = entity[rdfPredicate];
            if (Array.isArray(object)) {
              rdf = [ ...rdf, ...this.rdfQuadsFromJsonArray(object, term, predicateTerm) ];
            } else if (typeof object === 'object') {
              rdf = [ ...rdf, ...this.rdfQuadsFromJsonArray([ object ], term, predicateTerm) ];
            }
          });
      }
      return rdf;
    }, []);
  }

  private dgraphNodeToTerm(entity: DgraphNode): NamedNode | BlankNode | Literal {
    if ('blankNode' in entity && Boolean(entity.blankNode)) {
      return blankNode(entity.blankNode);
    }

    if ('uri' in entity) {
      return namedNode(entity.uri);
    }

    return this.dgraphNodeToLiteral(entity);
  }

  private dgraphNodeToLiteral(literalNode: DgraphLiteralNode): Literal {
    const valueKey = Object.keys(literalNode).find((key): boolean => key.startsWith('_value')) as ValuePredicate;
    const isNonString = literalNode.datatype &&
      literalNode.datatype !== 'http://www.w3.org/2001/XMLSchema#string' &&
      literalNode.datatype !== 'http://www.w3.org/1999/02/22-rdf-syntax-ns#langString';
    const datatypePart = isNonString ? `^^${literalNode.datatype}` : '';
    const languagePart = literalNode.language ? `@${literalNode.language}` : '';
    return new Literal(`"${literalNode[valueKey]}"${datatypePart}${languagePart}`);
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
    return quads.reduce((obj: Record<string, Quad[]>, rdfQuad): Record<string, Quad[]> => {
      if (!obj[rdfQuad.subject.value]) {
        obj[rdfQuad.subject.value] = [];
      }
      obj[rdfQuad.subject.value].push(rdfQuad);
      return obj;
    }, {});
  }
}
