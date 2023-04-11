# source-generator

**Title:** source-generator

|                           |                                                                           |
| ------------------------- | ------------------------------------------------------------------------- |
| **Type**                  | `object`                                                                  |
| **Required**              | No                                                                        |
| **Additional properties** | [[Any type: allowed]](# "Additional Properties of any type are allowed.") |

**Description:** the configuration specification of source-generator

| Property                   | Pattern | Type             | Deprecated | Definition | Title/Description |
| -------------------------- | ------- | ---------------- | ---------- | ---------- | ----------------- |
| - [stream](#stream )       | No      | string           | No         | -          | stream            |
| - [type](#type )           | No      | enum (of string) | No         | -          | type              |
| - [batchSize](#batchSize ) | No      | integer          | No         | -          | batchSize         |
| - [period](#period )       | No      | integer          | No         | -          | period            |

## <a name="stream"></a>1. Property `stream`

**Title:** stream

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | No       |

**Description:** result stream

## <a name="type"></a>2. Property `type`

**Title:** type

|              |                    |
| ------------ | ------------------ |
| **Type**     | `enum (of string)` |
| **Required** | No                 |

**Description:** generator type

Must be one of:
* "sequence"

## <a name="batchSize"></a>3. Property `batchSize`

**Title:** batchSize

|              |           |
| ------------ | --------- |
| **Type**     | `integer` |
| **Required** | No        |

**Description:** batch size

## <a name="period"></a>4. Property `period`

**Title:** period

|              |           |
| ------------ | --------- |
| **Type**     | `integer` |
| **Required** | No        |

**Description:** write period

----------------------------------------------------------------------------------------------------------------------------
