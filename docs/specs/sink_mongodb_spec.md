# sink-mongodb

**Title:** sink-mongodb

| Type                      | `object`                                                                  |
| ------------------------- | ------------------------------------------------------------------------- |
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

## <a name="user"></a>1. [Optional] Property `user`

| Type | `string` |
| ---- | -------- |

**Description:** user name

## <a name="password"></a>2. [Optional] Property `password`

| Type | `string` |
| ---- | -------- |

**Description:** password

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

## <a name="stream"></a>4. [Required] Property `stream`

| Type | `string` |
| ---- | -------- |

**Description:** source HStream stream name

## <a name="database"></a>5. [Required] Property `database`

| Type | `string` |
| ---- | -------- |

**Description:** target database name

## <a name="collection"></a>6. [Required] Property `collection`

| Type | `string` |
| ---- | -------- |

**Description:** connection name of the database

----------------------------------------------------------------------------------------------------------------------------
Generated using [json-schema-for-humans](https://github.com/coveooss/json-schema-for-humans) on 2022-09-16 at 18:53:20 +0800
