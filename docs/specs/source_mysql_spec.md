# source-mysql

**Title:** source-mysql

| Type                      | `object`                                                                  |
| ------------------------- | ------------------------------------------------------------------------- |
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

## <a name="user"></a>1. [Required] Property `user`

**Title:** User Name

| Type | `string` |
| ---- | -------- |

**Description:** mysql user name

## <a name="password"></a>2. [Required] Property `password`

**Title:** Password

| Type | `string` |
| ---- | -------- |

**Description:** password of the user

## <a name="host"></a>3. [Required] Property `host`

| Type | `string` |
| ---- | -------- |

**Description:** mysql hostname

**Examples:** 

```json
"127.0.0.1"
```

```json
"localhost"
```

## <a name="port"></a>4. [Required] Property `port`

**Title:** Port Number

| Type | `integer` |
| ---- | --------- |

**Description:** mysql port number

**Example:** 

```json
3306
```

## <a name="database"></a>5. [Required] Property `database`

**Title:** Database Name

| Type | `string` |
| ---- | -------- |

**Description:** mysql database name

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

**Title:** Stream Name

| Type | `string` |
| ---- | -------- |

**Description:** the target HStream stream name

----------------------------------------------------------------------------------------------------------------------------
Generated using [json-schema-for-humans](https://github.com/coveooss/json-schema-for-humans) on 2022-09-16 at 18:53:19 +0800
