export const MAX_TRANSACTION_RETRIES = 3;
export const INITIALIZATION_CHECK_PERIOD = 10;
export const MAX_INITIALIZATION_TIMEOUT_DURATION = 1500;
export const NON_RDF_KEYS = new Set([ 'uid', 'uri', 'container', 'dgraph.type' ]);
export const DEFAULT_SCHEMA = `
  <_definition>: uid .
  <type>: uid @reverse .
  <uri>: string @index(exact) .
  <container>: uid @reverse .
  <_timestamp>: uid .
  <_createdAt>: dateTime @index(hour) .
  <_updatedAt>: dateTime @index(hour) .

  type <Entity> {
    type
    uri
    container
    _createdAt
    _updatedAt
  }

  type <EntityData> {
    uri
    container
  }

  type <Metadata> {
    uri
    container
  }

  type <MetadataEntity> {
    uri
    container
  }

  type <Type> {
    _definition
  }
`;

export async function wait(duration: number): Promise<void> {
  return new Promise((resolve): void => {
    setTimeout(resolve, duration);
  });
}
