import { Subscription, timer } from 'rxjs';
import { share, takeUntil } from 'rxjs/operators';
import { createGroupedFeed } from './feed.js';
import {
  createMovingAverageStream,
  createSpikeAlertStream,
  createThrottledStream,
} from './analytics.js';

const INITIAL_PRICES = {
  AAPL: 190,
  MSFT: 420,
  TSLA: 250,
};

const groupedFeed$ = createGroupedFeed(INITIAL_PRICES);

const movingAvg$ = createMovingAverageStream(groupedFeed$);
const spikeAlert$ = createSpikeAlertStream(groupedFeed$);
const throttled$ = createThrottledStream(groupedFeed$);

const stop$ = timer(30000).pipe(share());
const subscription = new Subscription();

subscription.add(
  movingAvg$.pipe(takeUntil(stop$)).subscribe(avg => {
    console.log(
      `[Moving Avg] ${avg.symbol}: $${avg.movingAvg} at ${new Date(
        avg.timestamp
      ).toLocaleTimeString()}`
    );
  })
);

subscription.add(
  spikeAlert$.pipe(takeUntil(stop$)).subscribe(alert => {
    console.warn(
      `[Spike Alert] ${alert.symbol} changed ${alert.changePercent}% from $${alert.priceOld} to $${alert.priceNow}`
    );
  })
);

subscription.add(
  throttled$.pipe(takeUntil(stop$)).subscribe(tick => {
    console.log(
      `[Throttled Tick] ${tick.symbol}: $${tick.price} at ${new Date(
        tick.ts
      ).toLocaleTimeString()}`
    );
  })
);

stop$.subscribe(() => {
  subscription.unsubscribe();
  console.log('30 seconds elapsed. Unsubscribed from all streams.');
});