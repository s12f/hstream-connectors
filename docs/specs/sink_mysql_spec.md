# sink-mysql

**Title:** sink-mysql

| Type                      | `object`                                                                  |
| ------------------------- | ------------------------------------------------------------------------- |
| **Additional properties** | [[Any type: allowed]](# "Additional Properties of any type are allowed.") |

**Description:** the configuration specification of sink-mysql

| Property                 | Pattern | Type    | Deprecated | Definition | Title/Description          |
| ------------------------ | ------- | ------- | ---------- | ---------- | -------------------------- |
| + [user](#user )         | No      | string  | No         | -          | mysql user                 |
| + [password](#password ) | No      | string  | No         | -          | mysql password             |
| + [host](#host )         | No      | string  | No         | -          | mysql hostname             |
| + [port](#port )         | No      | integer | No         | -          | mysql port                 |
| + [stream](#stream )     | No      | string  | No         | -          | source HStream stream name |
| + [database](#database ) | No      | string  | No         | -          | target database name       |
| + [table](#table )       | No      | string  | No         | -          | table name of the database |

## <a name="user"></a>1. [Required] Property `user`

| Type | `string` |
| ---- | -------- |

**Description:** mysql user

## <a name="password"></a>2. [Required] Property `password`

| Type | `string` |
| ---- | -------- |

**Description:** mysql password

## <a name="host"></a>3. [Required] Property `host`

| Type | `string` |
| ---- | -------- |

**Description:** mysql hostname

## <a name="port"></a>4. [Required] Property `port`

| Type | `integer` |
| ---- | --------- |

**Description:** mysql port

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
Generated using [json-schema-for-humans](https://github.com/coveooss/json-schema-for-humans) on 2022-10-12 at 14:55:39 +0800
