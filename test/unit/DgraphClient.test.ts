import dgraph from 'dgraph-js';
import { DgraphClient } from '../../src/DgraphClient';
import * as DgraphUtil from '../../src/DgraphUtil';

jest.mock('dgraph-js');
const mockDgraph = dgraph as jest.Mocked<typeof dgraph>;

describe('A DgraphClient', (): void => {
  const connectionUri = 'localhost';
  const grpcPort = '9080';
  const vars = { var1: 'val1' };
  const query = 'query';
  const configuration = { connectionUri, grpcPort };
  let newTxn: jest.Mock<dgraph.Txn>;
  let alter: any;
  let setSchema: any;
  let queryWithVars: jest.Mock<Promise<dgraph.Response>>;
  let queryError: any;
  let updateError: any;
  let alterError: any;
  let doRequest: any;
  let discard: any;
  let dgraphJsonResponse: any;
  let client: DgraphClient;
  let setQuery: any;
  let setMutationsList: any;
  let setCommitNow: any;
  let setSetNquads: any;
  let setDelNquads: any;
  let operation: any;
  let request: any;
  let mutation: any;

  beforeEach(async(): Promise<void> => {
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
    alter = jest.fn((): void => {
      if (alterError) {
        throw alterError;
      }
    });
    setSchema = jest.fn();
    setMutationsList = jest.fn();
    setQuery = jest.fn();
    setCommitNow = jest.fn();
    setSetNquads = jest.fn();
    setDelNquads = jest.fn();

    mutation = { setSetNquads, setDelNquads };
    request = { setQuery, setMutationsList, setCommitNow };
    operation = { setSchema };

    (mockDgraph.DgraphClient as jest.Mock).mockReturnValue({ newTxn, alter });
    (mockDgraph.Mutation as unknown as jest.Mock).mockReturnValue(mutation);
    (mockDgraph.Request as unknown as jest.Mock).mockReturnValue(request);
    (mockDgraph.Operation as unknown as jest.Mock).mockReturnValue(operation);
    (mockDgraph.ERR_ABORTED as unknown) = new Error('Transaction has been aborted. Please retry');
    client = new DgraphClient(configuration);
  });

  it('creates a DgraphClient.', async(): Promise<void> => {
    expect(dgraph.DgraphClient).toHaveBeenCalled();
  });

  it('alters the schema.', async(): Promise<void> => {
    await expect(client.setSchema('schema')).resolves.toBeUndefined();
    expect(alter).toHaveBeenCalledWith(operation);
  });

  it('errors on setting the schema if the client cannot connect within the max timeout.', async(): Promise<void> => {
    jest.useFakeTimers();
    jest.mock('../../src/DgraphUtil');
    const mockDgraphUtil = DgraphUtil as jest.Mocked<typeof DgraphUtil>;
    (mockDgraphUtil.MAX_SCHEMA_ALTER_TIMEOUT_DURATION as unknown) = 0;

    alterError = new Error('14 UNAVAILABLE: No connection established');
    const promise = client.setSchema('schema').catch((error: any): void => {
      // eslint-disable-next-line jest/no-conditional-expect
      expect(error).toBe(alterError);
    });
    expect(alter).toHaveBeenCalledTimes(1);
    expect(alter).toHaveBeenCalledWith(operation);
    jest.advanceTimersByTime(DgraphUtil.SCHEMA_ALTER_ATTEMPT_PERIOD);
    await promise;
    expect(alter).toHaveBeenCalledTimes(2);
    jest.runAllTimers();
    jest.useRealTimers();
  });

  it('sends upsert queries.', async(): Promise<void> => {
    await expect(client.sendDgraphUpsert([ query ], [ 'delNQuad' ], [ 'setNquad' ])).resolves.toBeUndefined();
    expect(dgraph.Mutation).toHaveBeenCalledTimes(1);
    expect(setSetNquads).toHaveBeenCalledWith('setNquad');
    expect(setDelNquads).toHaveBeenCalledWith('delNQuad');
    expect(dgraph.Request).toHaveBeenCalledTimes(1);
    expect(setQuery).toHaveBeenCalledWith(`query { ${query} }`);
    expect(setMutationsList).toHaveBeenCalledWith([ mutation ]);
    expect(setCommitNow).toHaveBeenCalledWith(true);
    expect(newTxn).toHaveBeenCalledTimes(1);
    expect(doRequest).toHaveBeenCalledTimes(1);
    expect(doRequest).toHaveBeenCalledWith(request);
    expect(discard).toHaveBeenCalledTimes(1);
  });

  it('sends queries.', async(): Promise<void> => {
    dgraphJsonResponse = 'foobar';
    await expect(client.sendDgraphQuery(query, vars)).resolves.toBe(dgraphJsonResponse);
    expect(newTxn).toHaveBeenCalled();
    expect(queryWithVars).toHaveBeenCalledWith(query, vars);
    expect(discard).toHaveBeenCalled();
  });

  it('retries aborted query requests up to 3 times on abort errors.', async(): Promise<void> => {
    queryError = mockDgraph.ERR_ABORTED;
    await expect(client.sendDgraphQuery(query, vars)).rejects.toThrow(dgraph.ERR_ABORTED);

    expect(newTxn).toHaveBeenCalledTimes(3);
    expect(queryWithVars).toHaveBeenCalledTimes(3);
    expect(discard).toHaveBeenCalledTimes(3);

    queryError = undefined;
  });
});
