export const MAX_TRANSACTION_RETRIES = 3;
export const INITIALIZATION_CHECK_PERIOD = 10;
export const MAX_INITIALIZATION_TIMEOUT_DURATION = 1500;
export const NON_RDF_KEYS = new Set([ 'uid', 'uri', 'container', 'dgraph.type' ]);
export const DEFAULT_SCHEMA = `
  <_value.#i>: int @index(int) .
  <_value.#>: float @index(float) .
  <_value.?>: bool @index(bool) .
  <_value.%>: string @index(fulltext, trigram, hash) .
  <_value.%dt>: dateTime @index(hour) .
  <uri>: string @index(exact) .
  <container>: uid @reverse .

  type <Entity> {
    uri
    container
  }

  type <EntityData> {
    uri
    container
  }

  type <Metadata> {
    uri
    container
  }
`;

export enum ValuePredicate {
  datetime = '_value.%dt',
  string = '_value.%',
  boolean = '_value.?',
  integer = '_value.#i',
  float = '_value.#',
}

export interface DgraphConfiguration {
  connectionUri: string;
  grpcPort: string;
  schema?: string;
}

export function literalDatatypeToPrimitivePredicate(datatype: string): ValuePredicate {
  switch (datatype) {
    case 'http://www.w3.org/2001/XMLSchema#dateTime':
    case 'http://www.w3.org/2001/XMLSchema#date': {
      return ValuePredicate.datetime;
    }
    case 'http://www.w3.org/2001/XMLSchema#int':
    case 'http://www.w3.org/2001/XMLSchema#positiveInteger':
    case 'http://www.w3.org/2001/XMLSchema#integer': {
      return ValuePredicate.integer;
    }
    case 'http://www.w3.org/2001/XMLSchema#boolean': {
      return ValuePredicate.boolean;
    }
    case 'http://www.w3.org/2001/XMLSchema#double':
    case 'http://www.w3.org/2001/XMLSchema#float': {
      return ValuePredicate.float;
    }
    default: {
      return ValuePredicate.string;
    }
  }
}

export async function wait(duration: number): Promise<void> {
  return new Promise((resolve): void => {
    setTimeout(resolve, duration);
  });
}
