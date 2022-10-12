# source-sqlserver

**Title:** source-sqlserver

| Type                      | `object`                                                                  |
| ------------------------- | ------------------------------------------------------------------------- |
| **Additional properties** | [[Any type: allowed]](# "Additional Properties of any type are allowed.") |

**Description:** the configuration specification of source-sqlserver

| Property                 | Pattern | Type    | Deprecated | Definition | Title/Description |
| ------------------------ | ------- | ------- | ---------- | ---------- | ----------------- |
| + [user](#user )         | No      | string  | No         | -          | user name         |
| + [password](#password ) | No      | string  | No         | -          | password          |
| + [host](#host )         | No      | string  | No         | -          | Hostname          |
| + [port](#port )         | No      | integer | No         | -          | Port Number       |
| + [database](#database ) | No      | string  | No         | -          | Database Name     |
| + [table](#table )       | No      | string  | No         | -          | Table Name        |
| + [stream](#stream )     | No      | string  | No         | -          | Stream Name       |

## <a name="user"></a>1. [Required] Property `user`

**Title:** user name

| Type | `string` |
| ---- | -------- |

**Description:** sqlserver user name

## <a name="password"></a>2. [Required] Property `password`

**Title:** password

| Type | `string` |
| ---- | -------- |

**Description:** sqlserver password

## <a name="host"></a>3. [Required] Property `host`

**Title:** Hostname

| Type | `string` |
| ---- | -------- |

**Description:** sqlserver hostname

## <a name="port"></a>4. [Required] Property `port`

**Title:** Port Number

| Type | `integer` |
| ---- | --------- |

**Description:** sqlserver port number

**Example:** 

```json
1433
```

## <a name="database"></a>5. [Required] Property `database`

**Title:** Database Name

| Type | `string` |
| ---- | -------- |

**Description:** sqlserver database name

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
"dbo.t1"
```

## <a name="stream"></a>7. [Required] Property `stream`

**Title:** Stream Name

| Type | `string` |
| ---- | -------- |

**Description:** the target HStream stream name

----------------------------------------------------------------------------------------------------------------------------
Generated using [json-schema-for-humans](https://github.com/coveooss/json-schema-for-humans) on 2022-10-12 at 14:55:39 +0800
