package com.dpe.orchestrator.web.dto;

import java.util.List;

/**
 * A page of transfers and where to continue from.
 *
 * <p><b>There is no total count, and that is a decision rather than an omission.</b> A total means
 * {@code SELECT COUNT(*)} over every transfer the subject has ever sent, on every page request -
 * the one query in this endpoint whose cost is unbounded, added to pay for a number the UI renders
 * as "of 4,812". It is also a number that is wrong by the time it is rendered, because the list is
 * still being written to. If a total is ever genuinely needed it belongs in a periodically
 * refreshed counter, not on the page path.
 *
 * <p>{@code nextCursor} is null when this is the last page. That is the only "are there more"
 * signal a keyset API can give, and it is honest: the server knows there is at least one more row
 * because it asked for one more than it returned - see
 * {@code TransferService#listTransfers}.
 */
public record TransferPage(List<TransferSummary> items, String nextCursor) {
}
