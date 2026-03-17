# Payload Contracts — Эвотор события

## Product payload (товар)

Формат события при создании/обновлении товара в облаке Эвотор.

```json
{
  "type": "NORMAL",
  "id": "00000000-0000-0000-0000-000000000002",
  "name": "GP Alkaline AAx4",
  "code": "1",
  "price": 1.0,
  "cost_price": 0.99,
  "quantity": 0,
  "measure_name": "шт",
  "tax": "NO_VAT",
  "allow_to_sell": true,
  "description": "батарейки типа AA",
  "article_number": "",
  "classification_code": "",
  "quantity_in_package": 0,
  "is_excisable": false,
  "is_age_limited": false,
  "store_id": "00000000-0000-0000-0000-000000000001",
  "user_id": "01-000000000000001",
  "created_at": "2026-03-14T12:21:03.392+0000",
  "updated_at": "2026-03-14T12:21:03.392+0000",
  "barcodes": ["4800000000000"]
}
```

### Обязательные поля для обработки

| Поле | Тип | Описание |
|---|---|---|
| `id` | string (UUID) | Идентификатор товара в Эвотор |
| `name` | string | Название товара |
| `price` | float | Цена продажи (рубли) |
| `store_id` | string (UUID) | Идентификатор магазина |

### Необязательные поля

| Поле | Тип | Описание |
|---|---|---|
| `cost_price` | float | Закупочная цена |
| `quantity` | float | Остаток на складе |
| `measure_name` | string | Единица измерения |
| `tax` | string | Ставка НДС: `NO_VAT`, `VAT_10`, `VAT_20` и др. |
| `allow_to_sell` | bool | Разрешена ли продажа |
| `barcodes` | string[] | Штрихкоды товара |
| `code` | string | Внутренний код товара |

### Типы товаров (`type`)
- `NORMAL` — обычный товар
- `ALCOHOL_MARKED` — маркированный алкоголь
- `TOBACCO_PRODUCTS_MARKED` — маркированный табак
- `SERVICE` — услуга

---

## Stock payload (остатки)

Формат события при изменении остатков товара.

```json
{
  "id": "00000000-0000-0000-0000-000000000002",
  "store_id": "00000000-0000-0000-0000-000000000001",
  "user_id": "01-000000000000001",
  "quantity": 10.0
}
```

### Обязательные поля для обработки

| Поле | Тип | Описание |
|---|---|---|
| `id` | string (UUID) | Идентификатор товара в Эвотор |
| `store_id` | string (UUID) | Идентификатор магазина |
| `quantity` | float | Новый остаток |

---

## Sale payload (продажа) — для справки

Уже реализовано. Формат документа типа `SELL`.

```json
{
  "type": "SELL",
  "id": "00000000-0000-0000-0000-000000000002",
  "store_id": "00000000-0000-0000-0000-000000000001",
  "device_id": "00000000-0000-0000-0000-000000000003",
  "body": {
    "positions": [
      {
        "product_id": "00000000-0000-0000-0000-000000000002",
        "product_name": "GP Alkaline AAx4",
        "quantity": 1,
        "price": 1.0,
        "sum": 1.0
      }
    ],
    "sum": 1.0
  }
}
```

---

## МойСклад — что создаём для каждого события

| Событие Эвотор | Действие в МойСклад | Endpoint МС |
|---|---|---|
| `SELL` | Создать отгрузку (demand) | `POST /entity/demand` |
| `product` (новый) | Создать товар | `POST /entity/product` |
| `product` (обновление) | Обновить товар | `PUT /entity/product/{id}` |
| `stock` | Обновить остаток | `PUT /entity/store/{id}/quantity` |

---

## Маппинг полей: product Эвотор → МойСклад

| Эвотор | МойСклад | Примечание |
|---|---|---|
| `id` | `externalCode` | внешний код для идентификации |
| `name` | `name` | название товара |
| `price` | `salePrices[0].value` | цена в копейках (`* 100`) |
| `cost_price` | `buyPrice.value` | закупочная цена в копейках |
| `measure_name` | `uom.name` | единица измерения |
| `barcodes[0]` | `barcodes[0].ean13` | штрихкод |
| `description` | `description` | описание |
