import { DgraphClient as ActualDgraphClient } from 'dgraph-js';
import { DgraphClient } from '../../src/DgraphClient';
import { setGlobalDgraphClientInstance, getGlobalDgraphClientInstance } from '../../src/DgraphUtil';

jest.mock('dgraph-js');

describe('DgraphUtil', (): void => {
  const instance: any = {};
  let mockDgraphClient: any;
  beforeEach((): void => {
    jest.mock('../../src/DgraphClient');
    mockDgraphClient = DgraphClient as jest.Mocked<typeof DgraphClient>;
    mockDgraphClient.getInstance = jest.fn().mockReturnValue(instance);
  });

  it('allows setting the global dgraph client instance.', (): void => {
    const client = new ActualDgraphClient();
    setGlobalDgraphClientInstance(client);
    expect(mockDgraphClient.getInstance).toHaveBeenCalled();
    expect(instance.client).toEqual(client);
  });

  it('allows getting the global dgraph client instance.', (): void => {
    const client = new ActualDgraphClient();
    instance.client = client;
    expect(getGlobalDgraphClientInstance()).toEqual(client);
    expect(mockDgraphClient.getInstance).toHaveBeenCalled();
  });
});
