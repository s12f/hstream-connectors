# sink-postgresql

**Title:** sink-postgresql

|                           |                                                                           |
| ------------------------- | ------------------------------------------------------------------------- |
| **Type**                  | `object`                                                                  |
| **Required**              | No                                                                        |
| **Additional properties** | [[Any type: allowed]](# "Additional Properties of any type are allowed.") |

**Description:** the configuration specification of sink-postgresql

| Property                 | Pattern | Type    | Deprecated | Definition | Title/Description          |
| ------------------------ | ------- | ------- | ---------- | ---------- | -------------------------- |
| + [user](#user )         | No      | string  | No         | -          | postgresql user            |
| + [password](#password ) | No      | string  | No         | -          | postgresql password        |
| + [host](#host )         | No      | string  | No         | -          | postgresql hostname        |
| + [port](#port )         | No      | integer | No         | -          | postgresql port            |
| + [stream](#stream )     | No      | string  | No         | -          | source HStream stream name |
| + [database](#database ) | No      | string  | No         | -          | target database name       |
| + [table](#table )       | No      | string  | No         | -          | table name of the database |

## <a name="user"></a>1. Property `user`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** postgresql user

## <a name="password"></a>2. Property `password`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** postgresql password

## <a name="host"></a>3. Property `host`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** postgresql hostname

## <a name="port"></a>4. Property `port`

|              |           |
| ------------ | --------- |
| **Type**     | `integer` |
| **Required** | Yes       |

**Description:** postgresql port

## <a name="stream"></a>5. Property `stream`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** source HStream stream name

## <a name="database"></a>6. Property `database`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** target database name

## <a name="table"></a>7. Property `table`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** table name of the database

----------------------------------------------------------------------------------------------------------------------------
