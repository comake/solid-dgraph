import fs from 'fs';
import dgraph from 'dgraph-js';
import { DgraphClientInitializer } from '../../src/DgraphClientInitializer';
import * as DgraphUtil from '../../src/DgraphUtil';

jest.mock('dgraph-js');
const mockDgraph = dgraph as jest.Mocked<typeof dgraph>;

jest.mock('fs');
const mockFs = fs as jest.Mocked<typeof fs>;

const DGRAPH_SCHEMA = '<uri>: string @index(exact) .';

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
  schema: DGRAPH_SCHEMA,
});

describe('A DgraphClientInitializer', (): void => {
  let initializer: DgraphClientInitializer;
  let alter: any;
  let setSchema: any;
  const configFilePath = './dgraph-config.json';

  beforeEach(async(): Promise<void> => {
    (DgraphUtil.setGlobalDgraphClientInstance as jest.Mock) = jest.fn();
    alter = jest.fn();
    setSchema = jest.fn();
    (mockDgraph.DgraphClient as jest.Mock).mockReturnValue({ alter });
    (mockDgraph.Operation as unknown as jest.Mock).mockReturnValue({ setSchema });
    (mockFs.promises as unknown) = { readFile: jest.fn().mockReturnValue(CONFIG_WITHOUT_SCHEMA) };

    initializer = new DgraphClientInitializer(configFilePath);
  });

  it('creates a DgraphClient.', async(): Promise<void> => {
    await initializer.handle();
    expect(mockDgraph.DgraphClient).toHaveBeenCalledTimes(1);
  });

  it('sets the DgraphClient schema using the default schema.', async(): Promise<void> => {
    await initializer.handle();
    expect(alter).toHaveBeenCalledTimes(1);
    expect(setSchema).toHaveBeenCalledTimes(1);
    expect(setSchema).toHaveBeenCalledWith(DgraphUtil.DEFAULT_SCHEMA);
  });

  it('sets the DgraphClient schema using input schema.', async(): Promise<void> => {
    (mockFs.promises.readFile as jest.Mock).mockReturnValue(CONFIG_WITH_SCHEMA);
    await initializer.handle();
    expect(alter).toHaveBeenCalledTimes(1);
    expect(setSchema).toHaveBeenCalledTimes(1);
    expect(setSchema).toHaveBeenCalledWith(DGRAPH_SCHEMA);
  });

  it('sets the global DgraphClient instance.', async(): Promise<void> => {
    await initializer.handle();
    expect(DgraphUtil.setGlobalDgraphClientInstance).toHaveBeenCalledTimes(1);
  });
});
