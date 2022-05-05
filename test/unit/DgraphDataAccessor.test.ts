import 'jest-rdf';
import type { Readable } from 'stream';
import type { Guarded } from '@solid/community-server';
import { BasicRepresentation, RepresentationMetadata, NotFoundHttpError,
  NotImplementedHttpError, UnsupportedMediaTypeHttpError,
  SingleRootIdentifierStrategy, guardedStreamFrom, INTERNAL_QUADS,
  CONTENT_TYPE_TERM, LDP, RDF } from '@solid/community-server';
import arrayifyStream from 'arrayify-stream';
import { DataFactory, Literal } from 'n3';
import { DgraphClient } from '../../src/DgraphClient';
import { DgraphDataAccessor } from '../../src/DgraphDataAccessor';
import * as DgraphUtil from '../../src/DgraphUtil';

const { literal, namedNode, quad } = DataFactory;

jest.mock('../../src/DgraphClient');

const MOCK_DGRAPH_SCHEMA = '<uri>: string @index(exact) .';

const CONFIG_WITHOUT_SCHEMA = {
  connectionUri: 'localhost',
  grpcPort: '9080',
};

const CONFIG_WITH_SCHEMA = {
  ...CONFIG_WITHOUT_SCHEMA,
  schema: MOCK_DGRAPH_SCHEMA,
};

function simplifyQuery(query: string | string[]): string {
  if (Array.isArray(query)) {
    query = query.join(' ');
  }
  return query.replace(/\s+|\n/gu, ' ').trim();
}

describe('A DgraphDataAccessor', (): void => {
  const base = 'http://test.com/';
  const identifierStrategy = new SingleRootIdentifierStrategy(base);
  let accessor: DgraphDataAccessor;
  let metadata: RepresentationMetadata;
  let data: Guarded<Readable>;
  let setSchema: any;
  let sendDgraphQuery: any;
  let sendDgraphUpsert: any;

  let queryError: any;
  let updateError: any;
  let dgraphJsonResponse: any;

  beforeEach(async(): Promise<void> => {
    metadata = new RepresentationMetadata();
    data = guardedStreamFrom(
      [ quad(namedNode('http://name'), namedNode('http://pred'), literal('value')) ],
    );
    dgraphJsonResponse = {
      data: [{
        uid: '0x02',
        uri: 'http://this',
        'http://is.com/a': {
          uid: '0x03',
          '_value.%': 'triple',
        },
        'http://is.com/b': {
          uid: '0x04',
          uri: 'http://is.com/c',
          'dgraph.type': 'Entity',
        },
      }],
    };

    setSchema = jest.fn();

    // Makes it so the `queryWithVars` will always return `data`
    sendDgraphQuery = jest.fn(async(): Promise<any> => {
      if (queryError) {
        throw queryError;
      }
      return dgraphJsonResponse;
    });

    sendDgraphUpsert = jest.fn((): void => {
      if (updateError) {
        throw updateError;
      }
    });

    (DgraphClient as unknown as jest.Mock).mockReturnValue({ setSchema, sendDgraphQuery, sendDgraphUpsert });
  });

  describe('with input schema', (): void => {
    beforeEach(async(): Promise<void> => {
      accessor = new DgraphDataAccessor(identifierStrategy, CONFIG_WITH_SCHEMA);
    });

    it('sets the DgraphClient schema using input schema.', async(): Promise<void> => {
      await accessor.getData({ path: 'http://identifier' });
      expect(setSchema).toHaveBeenCalledTimes(1);
      expect(setSchema).toHaveBeenCalledWith(MOCK_DGRAPH_SCHEMA);
    });
  });

  describe('without input schema', (): void => {
    beforeEach(async(): Promise<void> => {
      accessor = new DgraphDataAccessor(identifierStrategy, CONFIG_WITHOUT_SCHEMA);
    });

    it('sets the DgraphClient schema using the default schema.', async(): Promise<void> => {
      await accessor.getData({ path: 'http://identifier' });
      expect(setSchema).toHaveBeenCalledTimes(1);
      expect(setSchema).toHaveBeenCalledWith(DgraphUtil.DEFAULT_SCHEMA);
    });

    it('errors during a query when the database fails to initialize within the max initialization timeout.', async():
    Promise<void> => {
      jest.useFakeTimers();
      jest.mock('../../src/DgraphUtil');
      const mockDgraphUtil = DgraphUtil as jest.Mocked<typeof DgraphUtil>;
      (mockDgraphUtil.MAX_INITIALIZATION_TIMEOUT_DURATION as unknown) = 0;

      // Make the schema alter operation take a long time
      setSchema.mockImplementation(
        async(): Promise<void> => new Promise((resolve): void => {
          setTimeout(resolve, 1000);
        }),
      );

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
      representation = new BasicRepresentation(data, 'new/internalType', false);
      await expect(accessor.canHandle(representation)).rejects.toThrow(UnsupportedMediaTypeHttpError);
      representation = new BasicRepresentation(data, INTERNAL_QUADS, false);
      metadata.contentType = INTERNAL_QUADS;
      await expect(accessor.canHandle(representation)).resolves.toBeUndefined();
    });

    it('returns the corresponding quads when data is requested.', async(): Promise<void> => {
      const result = await accessor.getData({ path: 'http://identifier' });
      await expect(arrayifyStream(result)).resolves.toBeRdfIsomorphic([
        quad(namedNode('http://this'), namedNode('http://is.com/a'), literal('triple')),
        quad(namedNode('http://this'), namedNode('http://is.com/b'), namedNode('http://is.com/c')),
      ]);

      expect(sendDgraphQuery).toHaveBeenCalledTimes(1);
      expect(simplifyQuery(sendDgraphQuery.mock.calls[0][0])).toBe(simplifyQuery([
        'query data($identifier: string) {',
        ' entity as var(func: eq(<uri>, $identifier)) @filter(eq(<dgraph.type>, "Entity"))',
        ' data(func: has(container)) @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "EntityData")) {',
        '   expand(_userpredicate_) {',
        '     expand(_userpredicate_)',
        '   }',
        ' }',
        '}',
      ]));
      expect(sendDgraphQuery.mock.calls[0][1]).toStrictEqual({ $identifier: 'http://identifier' });
    });

    it('should resolve json arrays to quads.', async(): Promise<void> => {
      dgraphJsonResponse = {
        data: [{
          uid: '0x02',
          uri: 'http://identifier',
          'http://www.w3.org/1999/02/22-rdf-syntax-ns#type': [
            {
              uri: 'http://www.w3.org/ns/ldp#Resource',
              'dgraph.type': [ 'Entity' ],
            },
            {
              uri: 'http://www.w3.org/ns/ldp#Container',
              'dgraph.type': [ 'Entity' ],
            },
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

    it('should resolve json node objects to quads.', async(): Promise<void> => {
      dgraphJsonResponse = {
        data: [{
          uid: '0x02',
          uri: 'http://identifier',
          'https://best.friend': {
            'dgraph.type': [ 'Entity' ],
            uri: 'http://otheridentifier',
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

    it('should resolve json literal objects to quads.', async(): Promise<void> => {
      dgraphJsonResponse = {
        data: [{
          uid: '0x02',
          uri: 'http://identifier',
          'https://best.friend': {
            uid: '0x03',
            datatype: 'http://www.w3.org/2001/XMLSchema#langString',
            language: 'en',
            '_value.%': 'triple',
          },
          'https://fav.number': {
            uid: '0x04',
            datatype: 'http://www.w3.org/2001/XMLSchema#integer',
            '_value.#i': '33',
          },
        }],
      };

      const result = await accessor.getData({ path: 'http://identifier' });
      await expect(arrayifyStream(result)).resolves.toBeRdfIsomorphic([
        quad(
          namedNode('http://identifier'),
          namedNode('https://best.friend'),
          new Literal('"triple"^^http://www.w3.org/2001/XMLSchema#langString@en'),
        ),
        quad(
          namedNode('http://identifier'),
          namedNode('https://fav.number'),
          new Literal('"33"^^http://www.w3.org/2001/XMLSchema#integer'),
        ),
      ]);
    });

    it('returns the corresponding metadata when requested.', async(): Promise<void> => {
      metadata = await accessor.getMetadata({ path: 'http://identifier' });
      expect(metadata.quads()).toBeRdfIsomorphic([
        quad(namedNode('http://this'), namedNode('http://is.com/a'), literal('triple')),
        quad(namedNode('http://this'), namedNode('http://is.com/b'), namedNode('http://is.com/c')),
        quad(namedNode('http://identifier'), CONTENT_TYPE_TERM, literal(INTERNAL_QUADS)),
      ]);

      expect(sendDgraphQuery).toHaveBeenCalledTimes(1);
      expect(simplifyQuery(sendDgraphQuery.mock.calls[0][0])).toBe(simplifyQuery([
        'query data($identifier: string) {',
        ' entity as var(func: eq(<uri>, $identifier)) @filter(eq(<dgraph.type>, "Entity"))',
        ' data(func: has(container)) @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "Metadata")) {',
        '   expand(_userpredicate_) {',
        '     expand(_userpredicate_)',
        '   }',
        ' }',
        '}',
      ]));
      expect(sendDgraphQuery.mock.calls[0][1]).toStrictEqual({ $identifier: 'http://identifier' });
    });

    it('does not set the content-type for container metadata.', async(): Promise<void> => {
      metadata = await accessor.getMetadata({ path: 'http://container/' });
      expect(metadata.quads()).toBeRdfIsomorphic([
        quad(namedNode('http://this'), namedNode('http://is.com/a'), literal('triple')),
        quad(namedNode('http://this'), namedNode('http://is.com/b'), namedNode('http://is.com/c')),
      ]);

      expect(sendDgraphQuery).toHaveBeenCalledTimes(1);
      expect(simplifyQuery(sendDgraphQuery.mock.calls[0][0])).toBe(simplifyQuery([
        'query data($identifier: string) {',
        ' entity as var(func: eq(<uri>, $identifier)) @filter(eq(<dgraph.type>, "Entity"))',
        ' data(func: has(container)) @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "Metadata")) {',
        '   expand(_userpredicate_) {',
        '     expand(_userpredicate_)',
        '   }',
        ' }',
        '}',
      ]));
      expect(sendDgraphQuery.mock.calls[0][1]).toStrictEqual({ $identifier: 'http://container/' });
    });

    it('throws 404 if no metadata was found.', async(): Promise<void> => {
      // Clear json response data
      dgraphJsonResponse = { data: []};
      await expect(accessor.getMetadata({ path: 'http://identifier' })).rejects.toThrow(NotFoundHttpError);

      expect(sendDgraphQuery).toHaveBeenCalledTimes(1);
      expect(simplifyQuery(sendDgraphQuery.mock.calls[0][0])).toBe(simplifyQuery([
        'query data($identifier: string) {',
        ' entity as var(func: eq(<uri>, $identifier)) @filter(eq(<dgraph.type>, "Entity"))',
        ' data(func: has(container)) @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "Metadata")) {',
        '   expand(_userpredicate_) {',
        '     expand(_userpredicate_)',
        '   }',
        ' }',
        '}',
      ]));
      expect(sendDgraphQuery.mock.calls[0][1]).toStrictEqual({ $identifier: 'http://identifier' });
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

      expect(sendDgraphQuery).toHaveBeenCalledTimes(1);
      expect(simplifyQuery(sendDgraphQuery.mock.calls[0][0])).toBe(simplifyQuery([
        'query data($identifier: string) {',
        ' entity as var(func: eq(<uri>, $identifier)) @filter(eq(<dgraph.type>, "Entity"))',
        ' data(func: eq(<dgraph.type>, "Entity")) @filter(uid_in(<container>, uid(entity))) {',
        '   uri',
        ' }',
        '}',
      ]));
      expect(sendDgraphQuery.mock.calls[0][1]).toStrictEqual({ $identifier: 'http://container/' });
    });

    it('overwrites the metadata when writing a container and updates parent.', async(): Promise<void> => {
      metadata = new RepresentationMetadata(
        { path: 'http://test.com/container/' },
        { [RDF.type]: [ LDP.terms.Resource, LDP.terms.Container ]},
      );
      await expect(accessor.writeContainer({ path: 'http://test.com/container/' }, metadata)).resolves.toBeUndefined();

      expect(sendDgraphUpsert).toHaveBeenCalledTimes(1);
      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][0])).toBe(simplifyQuery([
        'entity as var(func: eq(<uri>, "http://test.com/container/")) @filter(eq(<dgraph.type>, "Entity"))',
        'entityMetadata as var(func: has(container))',
        ' @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "Metadata"))',
        `Metadata00 as var(func: eq(<uri>, "${LDP.terms.Resource.value}")) @filter(eq(<dgraph.type>, "Entity"))`,
        `Metadata01 as var(func: eq(<uri>, "${LDP.terms.Container.value}")) @filter(eq(<dgraph.type>, "Entity"))`,
        'parent as var(func: eq(<uri>, "http://test.com/")) @filter(eq(<dgraph.type>, "Entity"))',
      ]));

      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][1])).toBe(
        simplifyQuery('uid(entityMetadata) * * .'),
      );
      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][2])).toBe(simplifyQuery([
        'uid(entity) <uri> "http://test.com/container/" .',
        'uid(entity) <dgraph.type> "Entity" .',
        '_:Metadata0 <uri> "http://test.com/container/" .',
        '_:Metadata0 <container> uid(entity) .',
        '_:Metadata0 <dgraph.type> "Metadata" .',
        `_:Metadata0 <${RDF.type}> uid(Metadata00) .`,
        `uid(Metadata00) <uri> "${LDP.terms.Resource.value}" .`,
        `uid(Metadata00) <dgraph.type> "Entity" .`,
        `_:Metadata0 <${RDF.type}> uid(Metadata01) .`,
        `uid(Metadata01) <uri> "${LDP.terms.Container.value}" .`,
        `uid(Metadata01) <dgraph.type> "Entity" .`,
        'uid(parent) <uri> "http://test.com/" .',
        'uid(parent) <dgraph.type> "Entity" .',
        'uid(entity) <container> uid(parent) .',
      ]));
    });

    it('does not write containment triples when writing to a root container.', async(): Promise<void> => {
      metadata = new RepresentationMetadata(
        { path: 'http://test.com/' },
        { [RDF.type]: [ LDP.terms.Resource, LDP.terms.Container ]},
      );
      await expect(accessor.writeContainer({ path: 'http://test.com/' }, metadata)).resolves.toBeUndefined();

      expect(sendDgraphUpsert).toHaveBeenCalledTimes(1);
      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][0])).toBe(simplifyQuery([
        'entity as var(func: eq(<uri>, "http://test.com/")) @filter(eq(<dgraph.type>, "Entity"))',
        'entityMetadata as var(func: has(container))',
        ' @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "Metadata"))',
        `Metadata00 as var(func: eq(<uri>, "${LDP.terms.Resource.value}")) @filter(eq(<dgraph.type>, "Entity"))`,
        `Metadata01 as var(func: eq(<uri>, "${LDP.terms.Container.value}")) @filter(eq(<dgraph.type>, "Entity"))`,
      ]));
      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][1])).toBe(
        simplifyQuery('uid(entityMetadata) * * .'),
      );
      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][2])).toBe(simplifyQuery([
        'uid(entity) <uri> "http://test.com/" .',
        'uid(entity) <dgraph.type> "Entity" .',
        '_:Metadata0 <uri> "http://test.com/" .',
        '_:Metadata0 <container> uid(entity) .',
        '_:Metadata0 <dgraph.type> "Metadata" .',
        `_:Metadata0 <${RDF.type}> uid(Metadata00) .`,
        `uid(Metadata00) <uri> "${LDP.terms.Resource.value}" .`,
        `uid(Metadata00) <dgraph.type> "Entity" .`,
        `_:Metadata0 <${RDF.type}> uid(Metadata01) .`,
        `uid(Metadata01) <uri> "${LDP.terms.Container.value}" .`,
        `uid(Metadata01) <dgraph.type> "Entity" .`,
      ]));
    });

    it('errors during an upsert when the database fails to initialize within the max initialization timeout.', async():
    Promise<void> => {
      jest.useFakeTimers();
      jest.mock('../../src/DgraphUtil');
      const mockDgraphUtil = DgraphUtil as jest.Mocked<typeof DgraphUtil>;
      (mockDgraphUtil.MAX_INITIALIZATION_TIMEOUT_DURATION as unknown) = 0;

      // Make the schema alter operation take a long time
      setSchema.mockImplementation(
        async(): Promise<void> => new Promise((resolve): void => {
          setTimeout(resolve, 1000);
        }),
      );

      metadata = new RepresentationMetadata(
        { path: 'http://test.com/container/' },
        { [RDF.type]: [ LDP.terms.Resource, LDP.terms.Container ]},
      );
      // Send a first request which will start initialization of database
      accessor.writeContainer({ path: 'http://test.com/container/' }, metadata).catch((): void => {
        // Do nothing
      });
      // Send a second request which should fail after waiting MAX_INITIALIZATION_TIMEOUT_DURATION
      const promise = accessor.writeContainer({ path: 'http://test.com/container/' }, metadata);
      jest.advanceTimersByTime(DgraphUtil.INITIALIZATION_CHECK_PERIOD);
      await expect(promise).rejects.toThrow('Failed to initialize Dgraph database.');
      jest.runAllTimers();
      jest.useRealTimers();
    });

    it('overwrites the data and metadata when writing a resource and updates parent.', async(): Promise<void> => {
      metadata = new RepresentationMetadata(
        { path: 'http://test.com/container/resource' },
        { [RDF.type]: [ LDP.terms.Resource ]},
      );
      await expect(accessor.writeDocument({ path: 'http://test.com/container/resource' }, data, metadata))
        .resolves.toBeUndefined();

      expect(sendDgraphUpsert).toHaveBeenCalledTimes(1);
      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][0])).toBe(simplifyQuery([
        'entity as var(func: eq(<uri>, "http://test.com/container/resource")) @filter(eq(<dgraph.type>, "Entity"))',
        'entityMetadata as var(func: has(container))',
        ' @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "Metadata"))',
        `Metadata00 as var(func: eq(<uri>, "${LDP.terms.Resource.value}")) @filter(eq(<dgraph.type>, "Entity"))`,
        'parent as var(func: eq(<uri>, "http://test.com/container/")) @filter(eq(<dgraph.type>, "Entity"))',
        'entityEntityData as var(func: has(container))',
        ' @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "EntityData"))',
        'EntityData00 as var(func: eq(<_value.%>, "value"))',
        ' @filter(eq(<language>, "") and eq(<datatype>, "http://www.w3.org/2001/XMLSchema#string"))',
      ]));
      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][1])).toBe(simplifyQuery([
        'uid(entityMetadata) * * .',
        'uid(entityEntityData) * * .',
      ]));
      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][2])).toBe(simplifyQuery([
        'uid(entity) <uri> "http://test.com/container/resource" .',
        'uid(entity) <dgraph.type> "Entity" .',
        '_:Metadata0 <uri> "http://test.com/container/resource" .',
        '_:Metadata0 <container> uid(entity) .',
        '_:Metadata0 <dgraph.type> "Metadata" .',
        `_:Metadata0 <${RDF.type}> uid(Metadata00) .`,
        `uid(Metadata00) <uri> "${LDP.terms.Resource.value}" .`,
        `uid(Metadata00) <dgraph.type> "Entity" .`,
        'uid(parent) <uri> "http://test.com/container/" .',
        'uid(parent) <dgraph.type> "Entity" .',
        'uid(entity) <container> uid(parent) .',
        '_:EntityData0 <uri> "http://name" .',
        '_:EntityData0 <container> uid(entity) .',
        '_:EntityData0 <dgraph.type> "EntityData" .',
        `_:EntityData0 <http://pred> uid(EntityData00) .`,
        `uid(EntityData00) <_value.%> "value" .`,
        `uid(EntityData00) <language> "" .`,
        `uid(EntityData00) <datatype> "http://www.w3.org/2001/XMLSchema#string" .`,
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

      expect(sendDgraphUpsert).toHaveBeenCalledTimes(1);
      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][0])).toBe(simplifyQuery([
        'entity as var(func: eq(<uri>, "http://test.com/container/resource")) @filter(eq(<dgraph.type>, "Entity"))',
        'entityMetadata as var(func: has(container))',
        ' @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "Metadata"))',
        `Metadata00 as var(func: eq(<uri>, "${LDP.terms.Resource.value}")) @filter(eq(<dgraph.type>, "Entity"))`,
        'parent as var(func: eq(<uri>, "http://test.com/container/")) @filter(eq(<dgraph.type>, "Entity"))',
        'entityEntityData as var(func: has(container))',
        ' @filter(uid_in(container, uid(entity)) and eq(<dgraph.type>, "EntityData"))',
      ]));
      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][1])).toBe(simplifyQuery([
        'uid(entityMetadata) * * .',
        'uid(entityEntityData) * * .',
      ]));
      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][2])).toBe(simplifyQuery([
        'uid(entity) <uri> "http://test.com/container/resource" .',
        'uid(entity) <dgraph.type> "Entity" .',
        '_:Metadata0 <uri> "http://test.com/container/resource" .',
        '_:Metadata0 <container> uid(entity) .',
        '_:Metadata0 <dgraph.type> "Metadata" .',
        `_:Metadata0 <${RDF.type}> uid(Metadata00) .`,
        `uid(Metadata00) <uri> "${LDP.terms.Resource.value}" .`,
        `uid(Metadata00) <dgraph.type> "Entity" .`,
        'uid(parent) <uri> "http://test.com/container/" .',
        'uid(parent) <dgraph.type> "Entity" .',
        'uid(entity) <container> uid(parent) .',
      ]));
    });

    it('escapes double quotes in literals.', async(): Promise<void> => {
      metadata = new RepresentationMetadata(
        { path: 'http://test.com/container/resource' },
        { 'http://is.com/a': [ literal('I think "trouble" is brewing') ]},
      );
      const empty = guardedStreamFrom([]);
      await expect(accessor.writeDocument({ path: 'http://test.com/container/resource' }, empty, metadata))
        .resolves.toBeUndefined();

      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][2])).toBe(simplifyQuery([
        'uid(entity) <uri> "http://test.com/container/resource" .',
        'uid(entity) <dgraph.type> "Entity" .',
        '_:Metadata0 <uri> "http://test.com/container/resource" .',
        '_:Metadata0 <container> uid(entity) .',
        '_:Metadata0 <dgraph.type> "Metadata" .',
        `_:Metadata0 <http://is.com/a> uid(Metadata00) .`,
        `uid(Metadata00) <_value.%> "I think \\"trouble\\" is brewing" .`,
        `uid(Metadata00) <language> "" .`,
        `uid(Metadata00) <datatype> "http://www.w3.org/2001/XMLSchema#string" .`,
        'uid(parent) <uri> "http://test.com/container/" .',
        'uid(parent) <dgraph.type> "Entity" .',
        'uid(entity) <container> uid(parent) .',
      ]));
    });

    it('writes date datatypes.', async(): Promise<void> => {
      metadata = new RepresentationMetadata(
        { path: 'http://test.com/container/resource' },
        { 'http://is.com/a': [ literal('2022-03-15T20:17:55Z', namedNode('http://www.w3.org/2001/XMLSchema#dateTime')) ]},
      );

      const empty = guardedStreamFrom([]);
      await expect(accessor.writeDocument({ path: 'http://test.com/container/resource' }, empty, metadata))
        .resolves.toBeUndefined();

      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][2])).toBe(simplifyQuery([
        'uid(entity) <uri> "http://test.com/container/resource" .',
        'uid(entity) <dgraph.type> "Entity" .',
        '_:Metadata0 <uri> "http://test.com/container/resource" .',
        '_:Metadata0 <container> uid(entity) .',
        '_:Metadata0 <dgraph.type> "Metadata" .',
        `_:Metadata0 <http://is.com/a> uid(Metadata00) .`,
        `uid(Metadata00) <_value.%dt> "2022-03-15T20:17:55Z" .`,
        `uid(Metadata00) <language> "" .`,
        `uid(Metadata00) <datatype> "http://www.w3.org/2001/XMLSchema#dateTime" .`,
        'uid(parent) <uri> "http://test.com/container/" .',
        'uid(parent) <dgraph.type> "Entity" .',
        'uid(entity) <container> uid(parent) .',
      ]));
    });

    it('writes integer datatypes.', async(): Promise<void> => {
      metadata = new RepresentationMetadata(
        { path: 'http://test.com/container/resource' },
        { 'http://is.com/a': [
          literal(13, namedNode('http://www.w3.org/2001/XMLSchema#integer')),
          literal(13, namedNode('http://www.w3.org/2001/XMLSchema#int')),
          literal(13, namedNode('http://www.w3.org/2001/XMLSchema#positiveInteger')),
        ]},
      );

      const empty = guardedStreamFrom([]);
      await expect(accessor.writeDocument({ path: 'http://test.com/container/resource' }, empty, metadata))
        .resolves.toBeUndefined();

      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][2])).toBe(simplifyQuery([
        'uid(entity) <uri> "http://test.com/container/resource" .',
        'uid(entity) <dgraph.type> "Entity" .',
        '_:Metadata0 <uri> "http://test.com/container/resource" .',
        '_:Metadata0 <container> uid(entity) .',
        '_:Metadata0 <dgraph.type> "Metadata" .',
        `_:Metadata0 <http://is.com/a> uid(Metadata00) .`,
        `uid(Metadata00) <_value.#i> "13" .`,
        `uid(Metadata00) <language> "" .`,
        `uid(Metadata00) <datatype> "http://www.w3.org/2001/XMLSchema#integer" .`,
        `_:Metadata0 <http://is.com/a> uid(Metadata01) .`,
        `uid(Metadata01) <_value.#i> "13" .`,
        `uid(Metadata01) <language> "" .`,
        `uid(Metadata01) <datatype> "http://www.w3.org/2001/XMLSchema#int" .`,
        `_:Metadata0 <http://is.com/a> uid(Metadata02) .`,
        `uid(Metadata02) <_value.#i> "13" .`,
        `uid(Metadata02) <language> "" .`,
        `uid(Metadata02) <datatype> "http://www.w3.org/2001/XMLSchema#positiveInteger" .`,
        'uid(parent) <uri> "http://test.com/container/" .',
        'uid(parent) <dgraph.type> "Entity" .',
        'uid(entity) <container> uid(parent) .',
      ]));
    });

    it('writes float datatypes.', async(): Promise<void> => {
      metadata = new RepresentationMetadata(
        { path: 'http://test.com/container/resource' },
        { 'http://is.com/a': [
          literal(13.55, namedNode('http://www.w3.org/2001/XMLSchema#float')),
          literal(13.55, namedNode('http://www.w3.org/2001/XMLSchema#double')),
        ]},
      );

      const empty = guardedStreamFrom([]);
      await expect(accessor.writeDocument({ path: 'http://test.com/container/resource' }, empty, metadata))
        .resolves.toBeUndefined();

      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][2])).toBe(simplifyQuery([
        'uid(entity) <uri> "http://test.com/container/resource" .',
        'uid(entity) <dgraph.type> "Entity" .',
        '_:Metadata0 <uri> "http://test.com/container/resource" .',
        '_:Metadata0 <container> uid(entity) .',
        '_:Metadata0 <dgraph.type> "Metadata" .',
        `_:Metadata0 <http://is.com/a> uid(Metadata00) .`,
        `uid(Metadata00) <_value.#> "13.55" .`,
        `uid(Metadata00) <language> "" .`,
        `uid(Metadata00) <datatype> "http://www.w3.org/2001/XMLSchema#float" .`,
        `_:Metadata0 <http://is.com/a> uid(Metadata01) .`,
        `uid(Metadata01) <_value.#> "13.55" .`,
        `uid(Metadata01) <language> "" .`,
        `uid(Metadata01) <datatype> "http://www.w3.org/2001/XMLSchema#double" .`,
        'uid(parent) <uri> "http://test.com/container/" .',
        'uid(parent) <dgraph.type> "Entity" .',
        'uid(entity) <container> uid(parent) .',
      ]));
    });

    it('writes boolean datatypes.', async(): Promise<void> => {
      metadata = new RepresentationMetadata(
        { path: 'http://test.com/container/resource' },
        { 'http://is.com/a': [ literal('true', namedNode('http://www.w3.org/2001/XMLSchema#boolean')) ]},
      );

      const empty = guardedStreamFrom([]);
      await expect(accessor.writeDocument({ path: 'http://test.com/container/resource' }, empty, metadata))
        .resolves.toBeUndefined();

      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][2])).toBe(simplifyQuery([
        'uid(entity) <uri> "http://test.com/container/resource" .',
        'uid(entity) <dgraph.type> "Entity" .',
        '_:Metadata0 <uri> "http://test.com/container/resource" .',
        '_:Metadata0 <container> uid(entity) .',
        '_:Metadata0 <dgraph.type> "Metadata" .',
        `_:Metadata0 <http://is.com/a> uid(Metadata00) .`,
        `uid(Metadata00) <_value.?> "true" .`,
        `uid(Metadata00) <language> "" .`,
        `uid(Metadata00) <datatype> "http://www.w3.org/2001/XMLSchema#boolean" .`,
        'uid(parent) <uri> "http://test.com/container/" .',
        'uid(parent) <dgraph.type> "Entity" .',
        'uid(entity) <container> uid(parent) .',
      ]));
    });

    it('writes arbitrary datatypes as strings.', async(): Promise<void> => {
      metadata = new RepresentationMetadata(
        { path: 'http://test.com/container/resource' },
        { 'http://is.com/a': [ literal('34938', namedNode('http://aims.fao.org/aos/agrovoc/AgrovocCode')) ]},
      );

      const empty = guardedStreamFrom([]);
      await expect(accessor.writeDocument({ path: 'http://test.com/container/resource' }, empty, metadata))
        .resolves.toBeUndefined();

      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][2])).toBe(simplifyQuery([
        'uid(entity) <uri> "http://test.com/container/resource" .',
        'uid(entity) <dgraph.type> "Entity" .',
        '_:Metadata0 <uri> "http://test.com/container/resource" .',
        '_:Metadata0 <container> uid(entity) .',
        '_:Metadata0 <dgraph.type> "Metadata" .',
        `_:Metadata0 <http://is.com/a> uid(Metadata00) .`,
        `uid(Metadata00) <_value.%> "34938" .`,
        `uid(Metadata00) <language> "" .`,
        `uid(Metadata00) <datatype> "http://aims.fao.org/aos/agrovoc/AgrovocCode" .`,
        'uid(parent) <uri> "http://test.com/container/" .',
        'uid(parent) <dgraph.type> "Entity" .',
        'uid(entity) <container> uid(parent) .',
      ]));
    });

    it('removes all references when deleting a resource.', async(): Promise<void> => {
      metadata = new RepresentationMetadata({ path: 'http://test.com/container/' },
        { [RDF.type]: [ LDP.terms.Resource, LDP.terms.Container ]});
      await expect(accessor.deleteResource({ path: 'http://test.com/container/' })).resolves.toBeUndefined();

      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][0])).toBe(simplifyQuery([
        'entity as var(func: eq(<uri>, "http://test.com/container/")) @filter(eq(<dgraph.type>, "Entity"))',
        'dataInEntity as var(func: has(container)) @filter(uid_in(container, uid(entity)))',
        'parent as var(func: eq(<uri>, "http://test.com/")) @filter(eq(<dgraph.type>, "Entity"))',
      ]));

      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][1])).toBe(simplifyQuery([
        'uid(entity) * * .',
        'uid(dataInEntity) * * .',
        `uid(entity) <container> uid(parent) .`,
      ]));
    });

    it('does not try to remove containment triples when deleting a root container.', async(): Promise<void> => {
      metadata = new RepresentationMetadata({ path: 'http://test.com/' },
        { [RDF.type]: [ LDP.terms.Resource, LDP.terms.Container ]});
      await expect(accessor.deleteResource({ path: 'http://test.com/' })).resolves.toBeUndefined();

      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][0])).toBe(simplifyQuery([
        'entity as var(func: eq(<uri>, "http://test.com/")) @filter(eq(<dgraph.type>, "Entity"))',
        'dataInEntity as var(func: has(container)) @filter(uid_in(container, uid(entity)))',
      ]));

      expect(simplifyQuery(sendDgraphUpsert.mock.calls[0][1])).toBe(simplifyQuery([
        'uid(entity) * * .',
        'uid(dataInEntity) * * .',
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
});
