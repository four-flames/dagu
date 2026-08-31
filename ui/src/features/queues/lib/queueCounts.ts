// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

import { components } from '@/api/v1/schema';

type Queue = components['schemas']['Queue'];

// queueMayHaveQueuedItems reports whether a queue can still hold queued entries.
//
// queuedCount is a lower bound whenever queuedCountCapped is set, so a capped
// zero means "unknown, possibly more" rather than "empty". That case is reached
// when running entries fill the server-side scan window before any queued entry
// is seen, and treating it as empty would stop the page fetching the remainder.
export function queueMayHaveQueuedItems(queue: Queue): boolean {
  return (queue.queuedCount || 0) > 0 || queue.queuedCountCapped === true;
}
