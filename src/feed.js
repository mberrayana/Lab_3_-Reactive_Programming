
import { Observable, merge } from 'rxjs';
import { distinctUntilChanged, groupBy, map, share } from 'rxjs/operators';

function createSymbolFeed(symbol, startPrice) {
  return new Observable(sub => {
    let price = startPrice;
    const id = setInterval(() => {
      price = +(price * (1 + (Math.random() - 0.5) * 0.01)).toFixed(2);
      sub.next({ symbol, price, ts: Date.now() });
    }, 50 + Math.random() * 150);

    return () => clearInterval(id);
  });
}

export function createGroupedFeed(initialPrices) {
  const mergedFeed$ = merge(
    ...Object.entries(initialPrices).map(([symbol, start]) =>
      createSymbolFeed(symbol, start)
    )
  ).pipe(share());

  return mergedFeed$.pipe(
    groupBy(tick => tick.symbol),
    map(group$ =>
      group$.pipe(distinctUntilChanged((a, b) => a.price === b.price))
    ),
    share()
  );
}