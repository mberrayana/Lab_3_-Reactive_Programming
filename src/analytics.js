import {
  bufferTime,
  filter,
  map,
  mergeMap,
  sampleTime,
  scan,
} from 'rxjs/operators';

/**
 * Emits a 5-second moving average every 1 second, per symbol.
 */
export function createMovingAverageStream(
  groupedFeed$,
  windowMs = 5000,
  intervalMs = 1000
) {
  return groupedFeed$.pipe(
    mergeMap(group$ =>
      group$.pipe(
        bufferTime(windowMs, intervalMs),
        filter(buffer => buffer.length > 0),
        map(buffer => {
          const avg =
            buffer.reduce((acc, t) => acc + t.price, 0) / buffer.length;
          return {
            symbol: buffer[0].symbol,
            movingAvg: +avg.toFixed(2),
            timestamp: Date.now(),
          };
        })
      )
    )
  );
}

/**
 * Emits an alert when a symbol's price changes by more than `thresholdPercent`
 * relative to the oldest tick within the `lookbackMs` window.
 */
export function createSpikeAlertStream(
  groupedFeed$,
  thresholdPercent = 2,
  lookbackMs = 60000
) {
  return groupedFeed$.pipe(
    mergeMap(group$ =>
      group$.pipe(
        scan(
          (state, tick) => {
            const now = tick.ts;
            const filtered = [...state.prices, tick].filter(
              t => now - t.ts <= lookbackMs
            );

            let alert = undefined;
            if (filtered.length > 1) {
              const oldPrice = filtered[0].price;
              const changePercent = ((tick.price - oldPrice) / oldPrice) * 100;
              if (Math.abs(changePercent) > thresholdPercent) {
                alert = {
                  symbol: tick.symbol,
                  changePercent: +changePercent.toFixed(2),
                  priceNow: tick.price,
                  priceOld: oldPrice,
                };
              }
            }
            return { prices: filtered, alert };
          },
          { prices: [] }
        ),
        filter(state => state.alert !== undefined),
        map(state => state.alert)
      )
    )
  );
}

/**
 * Throttles updates to at most once every `throttleMs` per symbol.
 */
export function createThrottledStream(groupedFeed$, throttleMs = 250) {
  return groupedFeed$.pipe(
    mergeMap(group$ => group$.pipe(sampleTime(throttleMs)))
  );
}