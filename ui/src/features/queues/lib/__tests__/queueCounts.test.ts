// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

import { describe, expect, it } from 'vitest';
import { components } from '@/api/v1/schema';
import { queueMayHaveQueuedItems } from '../queueCounts';

type Queue = components['schemas']['Queue'];

function queue(overrides: Partial<Queue>): Queue {
  return {
    name: 'q',
    type: 'dag-based',
    runningCount: 0,
    queuedCount: 0,
    running: [],
    ...overrides,
  } as Queue;
}

describe('queueMayHaveQueuedItems', () => {
  it('treats a capped count of zero as possibly non-empty', () => {
    // The server stopped scanning before reaching any queued entry, so zero is
    // a lower bound rather than a fact. Reading it as empty would leave the page
    // showing "No queued items" while entries are still waiting.
    expect(
      queueMayHaveQueuedItems(
        queue({ queuedCount: 0, queuedCountCapped: true })
      )
    ).toBe(true);
  });

  it('treats an exact count of zero as empty', () => {
    expect(queueMayHaveQueuedItems(queue({ queuedCount: 0 }))).toBe(false);
    expect(
      queueMayHaveQueuedItems(
        queue({ queuedCount: 0, queuedCountCapped: false })
      )
    ).toBe(false);
  });

  it('is true whenever entries were counted', () => {
    expect(queueMayHaveQueuedItems(queue({ queuedCount: 3 }))).toBe(true);
    expect(
      queueMayHaveQueuedItems(
        queue({ queuedCount: 500, queuedCountCapped: true })
      )
    ).toBe(true);
  });

  it('handles a missing count', () => {
    expect(queueMayHaveQueuedItems(queue({ queuedCount: undefined }))).toBe(
      false
    );
    expect(
      queueMayHaveQueuedItems(
        queue({ queuedCount: undefined, queuedCountCapped: true })
      )
    ).toBe(true);
  });
});
