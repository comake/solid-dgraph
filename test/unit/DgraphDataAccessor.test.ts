import 'jest-rdf';
import fs from 'fs';
import type { Readable } from 'stream';
import type { Guarded } from '@solid/community-server';
import { BasicRepresentation, RepresentationMetadata, NotFoundHttpError,
  NotImplementedHttpError, UnsupportedMediaTypeHttpError,
  SingleRootIdentifierStrategy, guardedStreamFrom, INTERNAL_QUADS,
  CONTENT_TYPE_TERM, LDP, RDF } from '@solid/community-server';
import arrayifyStream from 'arrayify-stream';
import dgraph from 'dgraph-js';
import { DataFactory } from 'n3';
import { DgraphDataAccessor } from '../../src/DgraphDataAccessor';
import * as DgraphUtil from '../../src/DgraphUtil';

const { literal, namedNode, quad } = DataFactory;

jest.mock('dgraph-js');
const mockDgraph = dgraph as jest.Mocked<typeof dgraph>;

jest.mock('fs');
const mockFs = fs as jest.Mocked<typeof fs>;

const MOCK_DGRAPH_SCHEMA = '<uri>: string @index(exact) .';

const CONFIG_WITHOUT_SCHEMA = JSON.stringify({
  connectionUri: 'localhost',
  ports: {
    grpc: '9080',
    zero: '6080',
  },
});

const CONFIG_WITH_SCHEMA = JSON.stringify({
  connectionUri: 'localhost',
  ports: {
    grpc: '9080',
    zero: '6080',
  },
  schema: MOCK_DGRAPH_SCHEMA,
});

function simplifyQuery(query: string | string[]): string {
  if (Array.isArray(query)) {
    query = query.join(' ');
  }
  return query.replace(/\s+|\n/gu, ' ').trim();
}

describe('A DgraphDataAccessor', (): void => {
  const base = 'http://test.com/';
  const configFilePath = './dgraph-config.json';
  const identifierStrategy = new SingleRootIdentifierStrategy(base);
  let accessor: DgraphDataAccessor;
  let metadata: RepresentationMetadata;
  let data: Guarded<Readable>;
  let newTxn: jest.Mock<dgraph.Txn>;
  let alter: any;
  let setSchema: any;
  let queryWithVars: jest.Mock<Promise<dgraph.Response>>;
  let queryError: any;
  let updateError: any;
  let doRequest: any;
  let discard: any;
  let dgraphJsonResponse: any;

  beforeEach(async(): Promise<void> => {
    metadata = new RepresentationMetadata();
    data = guardedStreamFrom(
      [ quad(namedNode('http://name'), namedNode('http://pred'), literal('value')) ],
    );
    dgraphJsonResponse = {
      data: [{
        uid: '0x02',
        uri: '<http://this>',
        'http://is.com/a': '"triple"',
      }],
    };
    // Makes it so the `queryWithVars` will always return `data`
    queryWithVars = jest.fn(async(): Promise<any> => {
      if (queryError) {
        throw queryError;
      }
      return { getJson: jest.fn((): any => dgraphJsonResponse) };
    });

    doRequest = jest.fn((): void => {
      if (updateError) {
        throw updateError;
      }
    });
    discard = jest.fn();
    newTxn = jest.fn((): any => ({ queryWithVars, doRequest, discard }));
    alter = jest.fn();
    setSchema = jest.fn();

    class MutationMock {
      public setNquads?: string;
      public delNquads?: string;

      public setSetNquads(setNquads: string): void {
        this.setNquads = setNquads;
      }

      public setDelNquads(delNquads: string): void {
        this.delNquads = delNquads;
      }
    }

    class RequestMock {
      public query?: string;
      public mutations: MutationMock[] = [];

      public setQuery(query: string): void {
        this.query = query;
      }

      public setMutationsList(mutations: MutationMock[]): void {
        this.mutations = mutations;
      }

      public setCommitNow(): void {
        // Do nothing
      }
    }

    (mockDgraph.DgraphClient as jest.Mock).mockReturnValue({ newTxn, alter });
    (mockDgraph.Mutation as unknown as jest.Mock).mockImplementation((): MutationMock => new MutationMock());
    (mockDgraph.Request as unknown as jest.Mock).mockImplementation((): RequestMock => new RequestMock());
    (mockDgraph.Operation as unknown as jest.Mock).mockReturnValue({ setSchema });
    (mockDgraph.ERR_ABORTED as unknown) = new Error('Transaction has been aborted. Please retry');
    (mockFs.promises as unknown) = { readFile: jest.fn().mockReturnValue(CONFIG_WITHOUT_SCHEMA) };

    accessor = new DgraphDataAccessor(configFilePath, identifierStrategy);
  });

  it('sets the DgraphClient schema using the default schema.', async(): Promise<void> => {
    await accessor.getData({ path: 'http://identifier' });
    expect(alter).toHaveBeenCalledTimes(1);
    expect(setSchema).toHaveBeenCalledTimes(1);
    expect(setSchema).toHaveBeenCalledWith(DgraphUtil.DEFAULT_SCHEMA);
  });

  it('sets the DgraphClient schema using input schema.', async(): Promise<void> => {
    (mockFs.promises.readFile as jest.Mock).mockReturnValue(CONFIG_WITH_SCHEMA);
    await accessor.getData({ path: 'http://identifier' });
    expect(alter).toHaveBeenCalledTimes(1);
    expect(setSchema).toHaveBeenCalledTimes(1);
    expect(setSchema).toHaveBeenCalledWith(MOCK_DGRAPH_SCHEMA);
  });

  it('errors when the database fails to initialize within the max initialization timeout.', async(): Promise<void> => {
    jest.useFakeTimers();
    jest.mock('../../src/DgraphUtil');
    const mockDgraphUtil = DgraphUtil as jest.Mocked<typeof DgraphUtil>;
    (mockDgraphUtil.MAX_INITIALIZATION_TIMEOUT_DURATION as unknown) = 0;

    // Make the schema alter operation take a long time
    alter.mockImplementation(
      async(): Promise<void> => new Promise((resolve): void => {
        setTimeout(resolve, 1000);
      }),
    );

    accessor = new DgraphDataAccessor(configFilePath, identifierStrategy);
    // Send a first request which will start initialization of database
    accessor.getData({ path: 'http://identifier' }).catch((): void => {
      // Do nothing
    });
    // Send a second request which should fail after waiting MAX_INITIALIZATION_TIMEOUT_DURATION
    const promise = accessor.getData({ path: 'http://identifier' });
    jest.advanceTimersByTime(DgraphUtil.INITIALIZATION_CHECK_PERIOD);
    await expect(promise).rejects.toThrow('Failed to initialize Dgraph database.');
    jest.runAllTimers();
    jest.useRealTimers();
  });

  it('can only handle quad data.', async(): Promise<void> => {
    let representation = new BasicRepresentation(data, metadata, true);
    await expect(accessor.canHandle(representation)).rejects.toThrow(UnsupportedMediaTypeHttpError);
    representation = new BasicRepresentation(data, 'newInternalType', false);
    await expect(accessor.canHandle(representation)).rejects.toThrow(UnsupportedMediaTypeHttpError);
    representation = new BasicRepresentation(data, INTERNAL_QUADS, false);
    metadata.contentType = INTERNAL_QUADS;
    await expect(accessor.canHandle(representation)).resolves.toBeUndefined();
  });

  it('returns the corresponding quads when data is requested.', async(): Promise<void> => {
    const result = await accessor.getData({ path: 'http://identifier' });
    await expect(arrayifyStream(result)).resolves.toBeRdfIsomorphic([
      quad(namedNode('http://this'), namedNode('http://is.com/a'), literal('triple')),
    ]);

    expect(queryWithVars).toHaveBeenCalledTimes(1);
    expect(simplifyQuery(queryWithVars.mock.calls[0][0])).toBe(simplifyQuery([
      'query data($identifier: string) {',
      ' entity as var(func: eq(uri, $identifier)) @filter(eq(dgraph.type, ["Entity", "MetadataEntity"]))',
      ' data(func: has(container)) @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "EntityData")) {',
      '   expand(_userpredicate_)',
      ' }',
      '}',
    ]));
    expect(queryWithVars.mock.calls[0][1]).toStrictEqual({ $identifier: '<http://identifier>' });
  });

  it('should retry aborted query requests up to 3 times on abort errors.', async(): Promise<void> => {
    queryError = mockDgraph.ERR_ABORTED;
    await expect(accessor.getData({ path: 'http://identifier' })).rejects.toThrow(dgraph.ERR_ABORTED);

    expect(newTxn).toHaveBeenCalledTimes(3);
    expect(queryWithVars).toHaveBeenCalledTimes(3);

    queryError = undefined;
  });

  it('should resolve json arrays to quads.', async(): Promise<void> => {
    dgraphJsonResponse = {
      data: [{
        uid: '0x02',
        uri: '<http://identifier>',
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#type': [
          '<http://www.w3.org/ns/ldp#Resource>',
          '<http://www.w3.org/ns/ldp#Container>',
        ],
      }],
    };

    const result = await accessor.getData({ path: 'http://identifier' });
    await expect(arrayifyStream(result)).resolves.toBeRdfIsomorphic([
      quad(
        namedNode('http://identifier'),
        namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
        namedNode('http://www.w3.org/ns/ldp#Resource'),
      ),
      quad(
        namedNode('http://identifier'),
        namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
        namedNode('http://www.w3.org/ns/ldp#Container'),
      ),
    ]);
  });

  it('should resolve json objects to quads.', async(): Promise<void> => {
    dgraphJsonResponse = {
      data: [{
        uid: '0x02',
        uri: '<http://identifier>',
        'https://best.friend': {
          'dgraph.type': [
            'Entity',
          ],
          uri: '<http://otheridentifier>',
        },
      }],
    };

    const result = await accessor.getData({ path: 'http://identifier' });
    await expect(arrayifyStream(result)).resolves.toBeRdfIsomorphic([
      quad(
        namedNode('http://identifier'),
        namedNode('https://best.friend'),
        namedNode('http://otheridentifier'),
      ),
    ]);
  });

  it('returns the corresponding metadata when requested.', async(): Promise<void> => {
    metadata = await accessor.getMetadata({ path: 'http://identifier' });
    expect(metadata.quads()).toBeRdfIsomorphic([
      quad(namedNode('http://this'), namedNode('http://is.com/a'), literal('triple')),
      quad(namedNode('http://identifier'), CONTENT_TYPE_TERM, literal(INTERNAL_QUADS)),
    ]);

    expect(queryWithVars).toHaveBeenCalledTimes(1);
    expect(simplifyQuery(queryWithVars.mock.calls[0][0])).toBe(simplifyQuery([
      'query data($identifier: string) {',
      ' entity as var(func: eq(uri, $identifier)) @filter(eq(dgraph.type, ["Entity", "MetadataEntity"]))',
      ' data(func: has(container)) @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "Metadata")) {',
      '   expand(_userpredicate_)',
      ' }',
      '}',
    ]));
    expect(queryWithVars.mock.calls[0][1]).toStrictEqual({ $identifier: '<http://identifier>' });
  });

  it('does not set the content-type for container metadata.', async(): Promise<void> => {
    metadata = await accessor.getMetadata({ path: 'http://container/' });
    expect(metadata.quads()).toBeRdfIsomorphic([
      quad(namedNode('http://this'), namedNode('http://is.com/a'), literal('triple')),
    ]);

    expect(queryWithVars).toHaveBeenCalledTimes(1);
    expect(simplifyQuery(queryWithVars.mock.calls[0][0])).toBe(simplifyQuery([
      'query data($identifier: string) {',
      ' entity as var(func: eq(uri, $identifier)) @filter(eq(dgraph.type, ["Entity", "MetadataEntity"]))',
      ' data(func: has(container)) @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "Metadata")) {',
      '   expand(_userpredicate_)',
      ' }',
      '}',
    ]));
    expect(queryWithVars.mock.calls[0][1]).toStrictEqual({ $identifier: '<http://container/>' });
  });

  it('throws 404 if no metadata was found.', async(): Promise<void> => {
    // Clear json response data
    dgraphJsonResponse = { data: []};
    await expect(accessor.getMetadata({ path: 'http://identifier' })).rejects.toThrow(NotFoundHttpError);

    expect(queryWithVars).toHaveBeenCalledTimes(1);
    expect(simplifyQuery(queryWithVars.mock.calls[0][0])).toBe(simplifyQuery([
      'query data($identifier: string) {',
      ' entity as var(func: eq(uri, $identifier)) @filter(eq(dgraph.type, ["Entity", "MetadataEntity"]))',
      ' data(func: has(container)) @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "Metadata")) {',
      '   expand(_userpredicate_)',
      ' }',
      '}',
    ]));
    expect(queryWithVars.mock.calls[0][1]).toStrictEqual({ $identifier: '<http://identifier>' });
  });

  it('requests the container data to find its children.', async(): Promise<void> => {
    dgraphJsonResponse = {
      data: [{
        uri: 'http://container/child',
      }],
    };

    const children = [];
    for await (const child of accessor.getChildren({ path: 'http://container/' })) {
      children.push(child);
    }
    expect(children).toHaveLength(1);
    expect(children[0].identifier.value).toBe('http://container/child');

    expect(queryWithVars).toHaveBeenCalledTimes(1);
    expect(simplifyQuery(queryWithVars.mock.calls[0][0])).toBe(simplifyQuery([
      'query data($identifier: string) {',
      ' entity as var(func: eq(uri, $identifier)) @filter(eq(<dgraph.type>, "Entity"))',
      ' data(func: eq(<dgraph.type>, "Entity")) @filter(uid_in(<container>, uid(entity))) {',
      '   uri',
      ' }',
      '}',
    ]));
    expect(queryWithVars.mock.calls[0][1]).toStrictEqual({ $identifier: '<http://container/>' });
  });

  it('overwrites the metadata when writing a container and updates parent.', async(): Promise<void> => {
    metadata = new RepresentationMetadata(
      { path: 'http://test.com/container/' },
      { [RDF.type]: [ LDP.terms.Resource, LDP.terms.Container ]},
    );
    await expect(accessor.writeContainer({ path: 'http://test.com/container/' }, metadata)).resolves.toBeUndefined();

    expect(doRequest).toHaveBeenCalledTimes(1);
    expect(simplifyQuery(doRequest.mock.calls[0][0].query)).toBe(simplifyQuery([
      'query {',
      ' entity as var(func: eq(uri, "<http://test.com/container/>")) @filter(eq(dgraph.type, "Entity"))',
      ' entityParent as var(func: eq(uri, "<http://test.com/>")) @filter(eq(dgraph.type, "Entity"))',
      ' entityMetaInName as var(func: has(container))',
      '   @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "Metadata"))',
      '}',
    ]));

    expect(simplifyQuery(doRequest.mock.calls[0][0].mutations[0].delNquads)).toBe(
      simplifyQuery('uid(entityMetaInName) * * .'),
    );
    expect(simplifyQuery(doRequest.mock.calls[0][0].mutations[1].setNquads)).toBe(simplifyQuery([
      'uid(entityParent) <uri> "<http://test.com/>" .',
      'uid(entityParent) <dgraph.type> "Entity" .',
      'uid(entity) <uri> "<http://test.com/container/>" .',
      'uid(entity) <dgraph.type> "Entity" .',
      '_:m0 <uri> "<http://test.com/container/>" .',
      '_:m0 <container> uid(entity) .',
      '_:m0 <dgraph.type> "Metadata" .',
      `_:m0 <${RDF.type}> "<${LDP.terms.Resource.value}>" .`,
      `_:m0 <${RDF.type}> "<${LDP.terms.Container.value}>" .`,
      'uid(entity) <container> uid(entityParent) .',
    ]));
  });

  it('does not write containment triples when writing to a root container.', async(): Promise<void> => {
    metadata = new RepresentationMetadata(
      { path: 'http://test.com/' },
      { [RDF.type]: [ LDP.terms.Resource, LDP.terms.Container ]},
    );
    await expect(accessor.writeContainer({ path: 'http://test.com/' }, metadata)).resolves.toBeUndefined();

    expect(doRequest).toHaveBeenCalledTimes(1);
    expect(simplifyQuery(doRequest.mock.calls[0][0].query)).toBe(simplifyQuery([
      'query {',
      ' entity as var(func: eq(uri, "<http://test.com/>")) @filter(eq(dgraph.type, "Entity"))',
      ' entityMetaInName as var(func: has(container))',
      '   @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "Metadata"))',
      '}',
    ]));
    expect(doRequest.mock.calls[0][0].mutations).toHaveLength(2);
    expect(simplifyQuery(doRequest.mock.calls[0][0].mutations[0].delNquads)).toBe(
      simplifyQuery('uid(entityMetaInName) * * .'),
    );
    expect(simplifyQuery(doRequest.mock.calls[0][0].mutations[1].setNquads)).toBe(simplifyQuery([
      'uid(entity) <uri> "<http://test.com/>" .',
      'uid(entity) <dgraph.type> "Entity" .',
      '_:m0 <uri> "<http://test.com/>" .',
      '_:m0 <container> uid(entity) .',
      '_:m0 <dgraph.type> "Metadata" .',
      `_:m0 <${RDF.type}> "<${LDP.terms.Resource.value}>" .`,
      `_:m0 <${RDF.type}> "<${LDP.terms.Container.value}>" .`,
    ]));
  });

  it('overwrites the data and metadata when writing a resource and updates parent.', async(): Promise<void> => {
    metadata = new RepresentationMetadata(
      { path: 'http://test.com/container/resource' },
      { [RDF.type]: [ LDP.terms.Resource ]},
    );
    await expect(accessor.writeDocument({ path: 'http://test.com/container/resource' }, data, metadata))
      .resolves.toBeUndefined();

    expect(doRequest).toHaveBeenCalledTimes(1);
    expect(simplifyQuery(doRequest.mock.calls[0][0].query)).toBe(simplifyQuery([
      'query {',
      ' entity as var(func: eq(uri, "<http://test.com/container/resource>")) @filter(eq(dgraph.type, "Entity"))',
      ' entityParent as var(func: eq(uri, "<http://test.com/container/>")) @filter(eq(dgraph.type, "Entity"))',
      ' entityDataInName as var(func: has(container))',
      '   @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "EntityData"))',
      ' entityMetaInName as var(func: has(container))',
      '   @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "Metadata"))',
      '}',
    ]));
    expect(doRequest.mock.calls[0][0].mutations).toHaveLength(3);
    expect(simplifyQuery(doRequest.mock.calls[0][0].mutations[0].delNquads)).toBe(
      simplifyQuery('uid(entityMetaInName) * * .'),
    );
    expect(simplifyQuery(doRequest.mock.calls[0][0].mutations[1].delNquads)).toBe(
      simplifyQuery('uid(entityDataInName) * * .'),
    );
    expect(simplifyQuery(doRequest.mock.calls[0][0].mutations[2].setNquads)).toBe(simplifyQuery([
      'uid(entityParent) <uri> "<http://test.com/container/>" .',
      'uid(entityParent) <dgraph.type> "Entity" .',
      'uid(entity) <uri> "<http://test.com/container/resource>" .',
      'uid(entity) <dgraph.type> "Entity" .',
      '_:m0 <uri> "<http://test.com/container/resource>" .',
      '_:m0 <container> uid(entity) .',
      '_:m0 <dgraph.type> "Metadata" .',
      `_:m0 <${RDF.type}> "<${LDP.terms.Resource.value}>" .`,
      'uid(entity) <container> uid(entityParent) .',
      '_:n0 <uri> "<http://name>" .',
      '_:n0 <container> uid(entity) .',
      '_:n0 <dgraph.type> "EntityData" .',
      '_:n0 <http://pred> "\\"value\\"" .',
    ]));
  });

  it('overwrites the data and metadata when writing an empty resource.', async(): Promise<void> => {
    metadata = new RepresentationMetadata(
      { path: 'http://test.com/container/resource' },
      { [RDF.type]: [ LDP.terms.Resource ]},
    );
    const empty = guardedStreamFrom([]);
    await expect(accessor.writeDocument({ path: 'http://test.com/container/resource' }, empty, metadata))
      .resolves.toBeUndefined();

    expect(doRequest).toHaveBeenCalledTimes(1);
    expect(simplifyQuery(doRequest.mock.calls[0][0].query)).toBe(simplifyQuery([
      'query {',
      ' entity as var(func: eq(uri, "<http://test.com/container/resource>")) @filter(eq(dgraph.type, "Entity"))',
      ' entityParent as var(func: eq(uri, "<http://test.com/container/>")) @filter(eq(dgraph.type, "Entity"))',
      ' entityDataInName as var(func: has(container))',
      '   @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "EntityData"))',
      ' entityMetaInName as var(func: has(container))',
      '   @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "Metadata"))',
      '}',
    ]));
    expect(doRequest.mock.calls[0][0].mutations).toHaveLength(3);
    expect(simplifyQuery(doRequest.mock.calls[0][0].mutations[0].delNquads)).toBe(
      simplifyQuery('uid(entityMetaInName) * * .'),
    );
    expect(simplifyQuery(doRequest.mock.calls[0][0].mutations[1].delNquads)).toBe(
      simplifyQuery('uid(entityDataInName) * * .'),
    );
    expect(simplifyQuery(doRequest.mock.calls[0][0].mutations[2].setNquads)).toBe(simplifyQuery([
      'uid(entityParent) <uri> "<http://test.com/container/>" .',
      'uid(entityParent) <dgraph.type> "Entity" .',
      'uid(entity) <uri> "<http://test.com/container/resource>" .',
      'uid(entity) <dgraph.type> "Entity" .',
      '_:m0 <uri> "<http://test.com/container/resource>" .',
      '_:m0 <container> uid(entity) .',
      '_:m0 <dgraph.type> "Metadata" .',
      `_:m0 <${RDF.type}> "<${LDP.terms.Resource.value}>" .`,
      'uid(entity) <container> uid(entityParent) .',
    ]));
  });

  it('retries aborted write requests up to 3 times on abort errors.', async(): Promise<void> => {
    const identifier = { path: 'http://test.com/container/resource' };
    metadata = new RepresentationMetadata(identifier);

    updateError = mockDgraph.ERR_ABORTED;
    await expect(accessor.writeDocument(identifier, data, metadata)).rejects.toThrow(dgraph.ERR_ABORTED);

    expect(newTxn).toHaveBeenCalledTimes(3);
    expect(doRequest).toHaveBeenCalledTimes(3);

    updateError = undefined;
  });

  it('removes all references when deleting a resource.', async(): Promise<void> => {
    metadata = new RepresentationMetadata({ path: 'http://test.com/container/' },
      { [RDF.type]: [ LDP.terms.Resource, LDP.terms.Container ]});
    await expect(accessor.deleteResource({ path: 'http://test.com/container/' })).resolves.toBeUndefined();

    expect(simplifyQuery(doRequest.mock.calls[0][0].query)).toBe(simplifyQuery([
      'query {',
      ' entity as var(func: eq(uri, "<http://test.com/container/>")) @filter(eq(dgraph.type, "Entity"))',
      ' entityParent as var(func: eq(uri, "<http://test.com/>")) @filter(eq(dgraph.type, "Entity"))',
      ' entitiesInName as var(func: has(container)) @filter(uid_in(container, uid(entity)))',
      '}',
    ]));

    expect(simplifyQuery(doRequest.mock.calls[0][0].mutations[0].delNquads)).toBe(simplifyQuery([
      'uid(entity) * * .',
      'uid(entitiesInName) * * .',
      `uid(entity) <container> uid(entityParent) .`,
    ]));
  });

  it('does not try to remove containment triples when deleting a root container.', async(): Promise<void> => {
    metadata = new RepresentationMetadata({ path: 'http://test.com/' },
      { [RDF.type]: [ LDP.terms.Resource, LDP.terms.Container ]});
    await expect(accessor.deleteResource({ path: 'http://test.com/' })).resolves.toBeUndefined();

    expect(simplifyQuery(doRequest.mock.calls[0][0].query)).toBe(simplifyQuery([
      'query {',
      ' entity as var(func: eq(uri, "<http://test.com/>")) @filter(eq(dgraph.type, "Entity"))',
      ' entitiesInName as var(func: has(container)) @filter(uid_in(container, uid(entity)))',
      '}',
    ]));

    expect(simplifyQuery(doRequest.mock.calls[0][0].mutations[0].delNquads)).toBe(simplifyQuery([
      'uid(entity) * * .',
      'uid(entitiesInName) * * .',
    ]));
  });

  it('errors when writing triples in a non-default graph.', async(): Promise<void> => {
    data = guardedStreamFrom(
      [ quad(namedNode('http://name'), namedNode('http://pred'), literal('value'), namedNode('badGraph!')) ],
    );
    const result = accessor.writeDocument({ path: 'http://test.com/container/resource' }, data, metadata);
    await expect(result).rejects.toThrow(NotImplementedHttpError);
    await expect(result).rejects.toThrow('Only triples in the default graph are supported.');
  });

  it('errors when the DGraph endpoint fails during reading.', async(): Promise<void> => {
    queryError = 'error';
    await expect(accessor.getMetadata({ path: 'http://identifier' })).rejects.toBe(queryError);

    queryError = new Error('read error');
    await expect(accessor.getMetadata({ path: 'http://identifier' })).rejects.toThrow(queryError);

    queryError = undefined;
  });

  it('errors when the DGraph endpoint fails during writing.', async(): Promise<void> => {
    const identifier = { path: 'http://test.com/container/' };
    metadata = new RepresentationMetadata(identifier);

    updateError = 'error';
    await expect(accessor.writeContainer(identifier, metadata)).rejects.toBe(updateError);

    updateError = new Error('write error');
    await expect(accessor.writeContainer(identifier, metadata)).rejects.toThrow(updateError);

    updateError = undefined;
  });
});
