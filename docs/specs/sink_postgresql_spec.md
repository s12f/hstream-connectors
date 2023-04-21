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
| + [stream](#stream )     | No      | string  | No         | -          | source HStream stream name |
| + [host](#host )         | No      | string  | No         | -          | postgresql hostname        |
| + [port](#port )         | No      | integer | No         | -          | postgresql port            |
| + [user](#user )         | No      | string  | No         | -          | postgresql user            |
| + [password](#password ) | No      | string  | No         | -          | postgresql password        |
| + [database](#database ) | No      | string  | No         | -          | target database name       |
| + [table](#table )       | No      | string  | No         | -          | table name of the database |

## <a name="stream"></a>1. Property `stream`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** source HStream stream name

## <a name="host"></a>2. Property `host`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** postgresql hostname

## <a name="port"></a>3. Property `port`

|              |           |
| ------------ | --------- |
| **Type**     | `integer` |
| **Required** | Yes       |
| **Default**  | `5432`    |

**Description:** postgresql port

## <a name="user"></a>4. Property `user`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** postgresql user

## <a name="password"></a>5. Property `password`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** postgresql password

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
