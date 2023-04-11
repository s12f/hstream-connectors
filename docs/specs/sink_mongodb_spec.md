# sink-mongodb

**Title:** sink-mongodb

|                           |                                                                           |
| ------------------------- | ------------------------------------------------------------------------- |
| **Type**                  | `object`                                                                  |
| **Required**              | No                                                                        |
| **Additional properties** | [[Any type: allowed]](# "Additional Properties of any type are allowed.") |

**Description:** the configuration specification of sink-mongodb

| Property                     | Pattern | Type   | Deprecated | Definition | Title/Description               |
| ---------------------------- | ------- | ------ | ---------- | ---------- | ------------------------------- |
| - [user](#user )             | No      | string | No         | -          | user name                       |
| - [password](#password )     | No      | string | No         | -          | password                        |
| + [hosts](#hosts )           | No      | string | No         | -          | hosts                           |
| + [stream](#stream )         | No      | string | No         | -          | source HStream stream name      |
| + [database](#database )     | No      | string | No         | -          | target database name            |
| + [collection](#collection ) | No      | string | No         | -          | connection name of the database |

## <a name="user"></a>1. Property `user`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | No       |

**Description:** user name

## <a name="password"></a>2. Property `password`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | No       |

**Description:** password

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

## <a name="stream"></a>4. Property `stream`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** source HStream stream name

## <a name="database"></a>5. Property `database`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** target database name

## <a name="collection"></a>6. Property `collection`

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** connection name of the database

----------------------------------------------------------------------------------------------------------------------------
