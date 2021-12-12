/**
 * A transform function to filter relevant tables and convert timestamp
 * @param {string} inJson
 * @return {string} outJson
 */
function transform(inJson) {
  var obj = JSON.parse(inJson);
  if (obj.hasOwnProperty('params')) {
      if (obj.params.hasOwnProperty('data')) {
          if (obj.params.data.hasOwnProperty('instrument_name')) {
            var data = obj.params.data;
            var ts = new Date();
            ts.setTime(data.timestamp);
            var parsed = {
                "timestamp": ts.toISOString(),
                "instrument_name": data.instrument_name,
                "bid": data.best_bid_price,
                "ask": data.best_ask_price,
                "bid_volume": data.best_bid_amount,
                "ask_volume": data.best_ask_amount
            };
            return JSON.stringify(parsed);
          }
    }
  }
}

