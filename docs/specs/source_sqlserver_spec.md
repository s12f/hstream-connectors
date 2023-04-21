# source-sqlserver

**Title:** source-sqlserver

|                           |                                                                           |
| ------------------------- | ------------------------------------------------------------------------- |
| **Type**                  | `object`                                                                  |
| **Required**              | No                                                                        |
| **Additional properties** | [[Any type: allowed]](# "Additional Properties of any type are allowed.") |

**Description:** the configuration specification of source-sqlserver

| Property                 | Pattern | Type    | Deprecated | Definition | Title/Description |
| ------------------------ | ------- | ------- | ---------- | ---------- | ----------------- |
| + [stream](#stream )     | No      | string  | No         | -          | Stream Name       |
| + [user](#user )         | No      | string  | No         | -          | user name         |
| + [password](#password ) | No      | string  | No         | -          | password          |
| + [host](#host )         | No      | string  | No         | -          | Hostname          |
| + [port](#port )         | No      | integer | No         | -          | Port Number       |
| + [database](#database ) | No      | string  | No         | -          | Database Name     |
| + [table](#table )       | No      | string  | No         | -          | Table Name        |

## <a name="stream"></a>1. Property `stream`

**Title:** Stream Name

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** the target HStream stream name

## <a name="user"></a>2. Property `user`

**Title:** user name

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** sqlserver user name

## <a name="password"></a>3. Property `password`

**Title:** password

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** sqlserver password

## <a name="host"></a>4. Property `host`

**Title:** Hostname

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** sqlserver hostname

## <a name="port"></a>5. Property `port`

**Title:** Port Number

|              |           |
| ------------ | --------- |
| **Type**     | `integer` |
| **Required** | Yes       |

**Description:** sqlserver port number

**Example:** 

```json
1433
```

## <a name="database"></a>6. Property `database`

**Title:** Database Name

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** sqlserver database name

## <a name="table"></a>7. Property `table`

**Title:** Table Name

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** table name of the database

**Examples:** 

```json
"t1"
```

```json
"dbo.t1"
```

----------------------------------------------------------------------------------------------------------------------------
