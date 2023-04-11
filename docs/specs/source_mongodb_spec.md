# source-mongodb

**Title:** source-mongodb

|                           |                                                                           |
| ------------------------- | ------------------------------------------------------------------------- |
| **Type**                  | `object`                                                                  |
| **Required**              | No                                                                        |
| **Additional properties** | [[Any type: allowed]](# "Additional Properties of any type are allowed.") |

**Description:** the configuration specification of source-mongodb

| Property                     | Pattern | Type   | Deprecated | Definition | Title/Description |
| ---------------------------- | ------- | ------ | ---------- | ---------- | ----------------- |
| - [user](#user )             | No      | string | No         | -          | User Name         |
| - [password](#password )     | No      | string | No         | -          | Password          |
| + [hosts](#hosts )           | No      | string | No         | -          | hosts             |
| + [database](#database )     | No      | string | No         | -          | Database Name     |
| + [collection](#collection ) | No      | string | No         | -          | Connection Name   |
| + [stream](#stream )         | No      | string | No         | -          | Stream Name       |

## <a name="user"></a>1. Property `user`

**Title:** User Name

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | No       |

**Description:** user name

## <a name="password"></a>2. Property `password`

**Title:** Password

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | No       |

**Description:** password of the user

## <a name="hosts"></a>3. Property `hosts`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** hosts

**Examples:** 

```json
"host1"
```

```json
"host1:1234"
```

```json
"host1:1234,host2:1234"
```

## <a name="database"></a>4. Property `database`

**Title:** Database Name

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** database name

## <a name="collection"></a>5. Property `collection`

**Title:** Connection Name

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** Connection name of the database

## <a name="stream"></a>6. Property `stream`

**Title:** Stream Name

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** the target HStream stream name

----------------------------------------------------------------------------------------------------------------------------
