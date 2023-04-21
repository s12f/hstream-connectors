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
| + [stream](#stream )     | No      | string  | No         | -          | Stream Name       |
| + [user](#user )         | No      | string  | No         | -          | User Name         |
| + [password](#password ) | No      | string  | No         | -          | Password          |
| + [host](#host )         | No      | string  | No         | -          | mysql hostname    |
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

**Title:** User Name

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** mysql user name

## <a name="password"></a>3. Property `password`

**Title:** Password

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** password of the user

## <a name="host"></a>4. Property `host`

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

## <a name="port"></a>5. Property `port`

**Title:** Port Number

|              |           |
| ------------ | --------- |
| **Type**     | `integer` |
| **Required** | Yes       |
| **Default**  | `3306`    |

**Description:** mysql port number

**Example:** 

```json
3306
```

## <a name="database"></a>6. Property `database`

**Title:** Database Name

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** mysql database name

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
"public.t1"
```

----------------------------------------------------------------------------------------------------------------------------
