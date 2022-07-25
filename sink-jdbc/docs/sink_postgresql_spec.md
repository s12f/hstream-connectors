# sink-postgresql

**Title:** sink-postgresql

| Type                      | `object`                                                                  |
| ------------------------- | ------------------------------------------------------------------------- |
| **Additional properties** | [[Any type: allowed]](# "Additional Properties of any type are allowed.") |

**Description:** the configuration specification of sink-postgresql

| Property                 | Pattern | Type    | Deprecated | Definition | Title/Description          |
| ------------------------ | ------- | ------- | ---------- | ---------- | -------------------------- |
| + [user](#user )         | No      | string  | No         | -          | postgresql user            |
| + [password](#password ) | No      | string  | No         | -          | postgresql password        |
| + [host](#host )         | No      | string  | No         | -          | postgresql hostname        |
| + [port](#port )         | No      | integer | No         | -          | postgresql port            |
| + [stream](#stream )     | No      | string  | No         | -          | source HStream stream name |
| + [database](#database ) | No      | string  | No         | -          | target database name       |
| + [table](#table )       | No      | string  | No         | -          | table name of the database |

## <a name="user"></a>1. [Required] Property `user`

| Type | `string` |
| ---- | -------- |

**Description:** postgresql user

## <a name="password"></a>2. [Required] Property `password`

| Type | `string` |
| ---- | -------- |

**Description:** postgresql password

## <a name="host"></a>3. [Required] Property `host`

| Type | `string` |
| ---- | -------- |

**Description:** postgresql hostname

## <a name="port"></a>4. [Required] Property `port`

| Type | `integer` |
| ---- | --------- |

**Description:** postgresql port

## <a name="stream"></a>5. [Required] Property `stream`

| Type | `string` |
| ---- | -------- |

**Description:** source HStream stream name

## <a name="database"></a>6. [Required] Property `database`

| Type | `string` |
| ---- | -------- |

**Description:** target database name

## <a name="table"></a>7. [Required] Property `table`

| Type | `string` |
| ---- | -------- |

**Description:** table name of the database

----------------------------------------------------------------------------------------------------------------------------
Generated using [json-schema-for-humans](https://github.com/coveooss/json-schema-for-humans) on 2022-07-22 at 18:13:26 +0800
