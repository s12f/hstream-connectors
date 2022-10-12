# source-postgresql

**Title:** source-postgresql

| Type                      | `object`                                                                  |
| ------------------------- | ------------------------------------------------------------------------- |
| **Additional properties** | [[Any type: allowed]](# "Additional Properties of any type are allowed.") |

**Description:** the configuration specification of source-postgresql

| Property                 | Pattern | Type    | Deprecated | Definition | Title/Description |
| ------------------------ | ------- | ------- | ---------- | ---------- | ----------------- |
| + [user](#user )         | No      | string  | No         | -          | User Name         |
| + [password](#password ) | No      | string  | No         | -          | Password          |
| + [host](#host )         | No      | string  | No         | -          | Hostname          |
| + [port](#port )         | No      | integer | No         | -          | Port Number       |
| + [database](#database ) | No      | string  | No         | -          | Database Name     |
| + [table](#table )       | No      | string  | No         | -          | Table Name        |
| + [stream](#stream )     | No      | string  | No         | -          | stream name       |

## <a name="user"></a>1. [Required] Property `user`

**Title:** User Name

| Type | `string` |
| ---- | -------- |

**Description:** postgresql user name

## <a name="password"></a>2. [Required] Property `password`

**Title:** Password

| Type | `string` |
| ---- | -------- |

**Description:** password of the user

## <a name="host"></a>3. [Required] Property `host`

**Title:** Hostname

| Type | `string` |
| ---- | -------- |

**Description:** postgresql hostname

**Example:** 

```json
"127.0.0.1"
```

## <a name="port"></a>4. [Required] Property `port`

**Title:** Port Number

| Type | `integer` |
| ---- | --------- |

**Description:** postgresql port number

**Example:** 

```json
5432
```

## <a name="database"></a>5. [Required] Property `database`

**Title:** Database Name

| Type | `string` |
| ---- | -------- |

**Description:** postgresql database name

## <a name="table"></a>6. [Required] Property `table`

**Title:** Table Name

| Type | `string` |
| ---- | -------- |

**Description:** table name of the database

**Examples:** 

```json
"t1"
```

```json
"public.t1"
```

## <a name="stream"></a>7. [Required] Property `stream`

**Title:** stream name

| Type | `string` |
| ---- | -------- |

**Description:** the target HStream stream name

----------------------------------------------------------------------------------------------------------------------------
Generated using [json-schema-for-humans](https://github.com/coveooss/json-schema-for-humans) on 2022-10-12 at 14:55:39 +0800
