# sink-mysql

**Title:** sink-mysql

|                           |                                                                           |
| ------------------------- | ------------------------------------------------------------------------- |
| **Type**                  | `object`                                                                  |
| **Required**              | No                                                                        |
| **Additional properties** | [[Any type: allowed]](# "Additional Properties of any type are allowed.") |

**Description:** the configuration specification of sink-mysql

| Property                 | Pattern | Type    | Deprecated | Definition | Title/Description          |
| ------------------------ | ------- | ------- | ---------- | ---------- | -------------------------- |
| + [stream](#stream )     | No      | string  | No         | -          | source HStream stream name |
| + [host](#host )         | No      | string  | No         | -          | mysql hostname             |
| + [port](#port )         | No      | integer | No         | -          | mysql port                 |
| + [user](#user )         | No      | string  | No         | -          | mysql user                 |
| + [password](#password ) | No      | string  | No         | -          | mysql password             |
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

**Description:** mysql hostname

## <a name="port"></a>3. Property `port`

|              |           |
| ------------ | --------- |
| **Type**     | `integer` |
| **Required** | Yes       |
| **Default**  | `3306`    |

**Description:** mysql port

## <a name="user"></a>4. Property `user`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** mysql user

## <a name="password"></a>5. Property `password`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** mysql password

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
