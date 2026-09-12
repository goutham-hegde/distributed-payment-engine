package com.dpe.account.saga;

import com.dpe.account.domain.AccountType;
import com.dpe.account.repository.AccountRepository;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * M8: which CLEARING shard a transfer's money is parked in.
 *
 * <p>One CLEARING row made every reserve and every commit in the system queue on one row lock (see
 * {@code V8__sharded_clearing.sql}). With several, two concurrent transfers only meet when they
 * choose the same one.
 *
 * <p><b>Chosen here only at RESERVE.</b> The choice is written onto the hold and commit and release
 * read it back from there - they must never call this. A shard derived again later would be the same
 * one only as long as the list of shards never changed.
 *
 * <p>The choice is a hash of the transfer id, not random, so a reserve that rolled back and is
 * redelivered lands on the same shard as its first attempt - which makes a transfer's legs easy to
 * find by hand, and costs nothing. Correctness does not depend on it.
 *
 * <p>The shards are read once per currency and kept. They are rows seeded by migrations, so the list
 * changes only with a deploy, and a stale list could only ever pick a shard that exists: this
 * reads, it never invents an id. An empty result is not cached, so a currency with no clearing
 * account keeps getting an honest "none" rather than a remembered one.
 */
@Component
public class ClearingAccounts {

    private final AccountRepository accounts;
    private final Map<String, List<UUID>> shardsByCurrency = new ConcurrentHashMap<>();

    public ClearingAccounts(AccountRepository accounts) {
        this.accounts = accounts;
    }

    /**
     * @return the shard for this transfer, or empty if there is no CLEARING account in this
     *         currency - which the reserve answers as a currency mismatch, exactly as it did when
     *         there was one clearing account in one currency
     */
    public Optional<UUID> forTransfer(UUID transferId, String currency) {
        List<UUID> shards = shards(currency);
        if (shards.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(shards.get(Math.floorMod(transferId.hashCode(), shards.size())));
    }

    List<UUID> shards(String currency) {
        List<UUID> cached = shardsByCurrency.get(currency);
        if (cached != null) {
            return cached;
        }
        List<UUID> loaded = List.copyOf(
                accounts.findIdsByTypeAndCurrency(AccountType.CLEARING, currency));
        if (!loaded.isEmpty()) {
            shardsByCurrency.putIfAbsent(currency, loaded);
        }
        return loaded;
    }
}
