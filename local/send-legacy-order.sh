#!/usr/bin/env sh
# Puts one legacy XML order on Artemis TB.ORDERS.OUT via the Artemis REST/Jolokia-free path:
# uses the broker's built-in `artemis producer` CLI inside the container.
#   ./local/send-legacy-order.sh [orderNbr] [storeNbr]
set -eu
ORDER="${1:-ORD-LOCAL-$(date +%s)}"
STORE="${2:-0412}"
XML="<Order><OrderNbr>${ORDER}</OrderNbr><OrderType>T</OrderType><StoreNbr>${STORE}</StoreNbr><CustNbr>C-1</CustNbr><OrderDate>$(date -u +%Y-%m-%dT%H:%M:%SZ)</OrderDate><Currency>USD</Currency><TotalAmt>649.99</TotalAmt><Lines><Line><LineNbr>1</LineNbr><SKU>MW-SUIT-NAVY-42R</SKU><Qty>1</Qty><UnitPrice>599.99</UnitPrice><FulfillType>P</FulfillType></Line><Line><LineNbr>2</LineNbr><SKU>ALT-HEM-TROUSER</SKU><Qty>1</Qty><UnitPrice>50.00</UnitPrice><FulfillType>A</FulfillType><Alteration><AltType>HEM</AltType><Measure>31.5</Measure><TailorShop>TS-EASTBAY</TailorShop></Alteration></Line></Lines></Order>"
docker compose exec -T artemis /var/lib/artemis-instance/bin/artemis producer \
  --url tcp://localhost:61616 --user artemis --password artemis \
  --destination queue://TB.ORDERS.OUT --message-count 1 --message "${XML}"
echo "sent ${ORDER} for store ${STORE}"
