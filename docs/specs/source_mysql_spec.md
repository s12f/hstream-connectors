# source-mysql

**Title:** source-mysql

|                           |                                                                           |
| ------------------------- | ------------------------------------------------------------------------- |
| **Type**                  | `object`                                                                  |
| **Required**              | No                                                                        |
| **Additional properties** | [[Any type: allowed]](# "Additional Properties of any type are allowed.") |

**Description:** the configuration specification of source-mysql

| Property                 | Pattern | Type    | Deprecated | Definition | Title/Description |
| ------------------------ | ------- | ------- | ---------- | ---------- | ----------------- |
| + [user](#user )         | No      | string  | No         | -          | User Name         |
| + [password](#password ) | No      | string  | No         | -          | Password          |
| + [host](#host )         | No      | string  | No         | -          | mysql hostname    |
| + [port](#port )         | No      | integer | No         | -          | Port Number       |
| + [database](#database ) | No      | string  | No         | -          | Database Name     |
| + [table](#table )       | No      | string  | No         | -          | Table Name        |
| + [stream](#stream )     | No      | string  | No         | -          | Stream Name       |

## <a name="user"></a>1. Property `user`

**Title:** User Name

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** mysql user name

## <a name="password"></a>2. Property `password`

**Title:** Password

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** password of the user

## <a name="host"></a>3. Property `host`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** mysql hostname

**Examples:** 

```json
"127.0.0.1"
```

```json
"localhost"
```

## <a name="port"></a>4. Property `port`

**Title:** Port Number

|              |           |
| ------------ | --------- |
| **Type**     | `integer` |
| **Required** | Yes       |

**Description:** mysql port number

**Example:** 

```json
3306
```

## <a name="database"></a>5. Property `database`

**Title:** Database Name

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** mysql database name

## <a name="table"></a>6. Property `table`

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
"public.t1"
```

## <a name="stream"></a>7. Property `stream`

**Title:** Stream Name

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** the target HStream stream name

----------------------------------------------------------------------------------------------------------------------------
