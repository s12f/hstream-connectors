# source-mongodb

**Title:** source-mongodb

| Type                      | `object`                                                                  |
| ------------------------- | ------------------------------------------------------------------------- |
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

## <a name="user"></a>1. [Optional] Property `user`

**Title:** User Name

| Type | `string` |
| ---- | -------- |

**Description:** user name

## <a name="password"></a>2. [Optional] Property `password`

**Title:** Password

| Type | `string` |
| ---- | -------- |

**Description:** password of the user

## <a name="hosts"></a>3. [Required] Property `hosts`

| Type | `string` |
| ---- | -------- |

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

## <a name="database"></a>4. [Required] Property `database`

**Title:** Database Name

| Type | `string` |
| ---- | -------- |

**Description:** database name

## <a name="collection"></a>5. [Required] Property `collection`

**Title:** Connection Name

| Type | `string` |
| ---- | -------- |

**Description:** Connection name of the database

## <a name="stream"></a>6. [Required] Property `stream`

**Title:** Stream Name

| Type | `string` |
| ---- | -------- |

**Description:** the target HStream stream name

----------------------------------------------------------------------------------------------------------------------------
Generated using [json-schema-for-humans](https://github.com/coveooss/json-schema-for-humans) on 2022-10-12 at 14:55:39 +0800
