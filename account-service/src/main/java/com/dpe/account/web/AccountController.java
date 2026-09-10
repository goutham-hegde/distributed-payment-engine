package com.dpe.account.web;

import com.dpe.account.domain.Account;
import com.dpe.account.service.AccountService;
import com.dpe.account.web.dto.AccountResponse;
import com.dpe.account.web.dto.CreateAccountRequest;
import com.dpe.account.web.dto.LedgerPage;
import com.dpe.security.Roles;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Accounts, and - as of M6 part 3 - the ledger history behind them.
 *
 * <h2>Why the reads take a principal and the write does not</h2>
 *
 * <p>{@code POST /accounts} is OPERATOR-only in the filter chain and needs no per-request check:
 * the role IS the whole answer, because opening an account in somebody's name is an operator
 * action whichever account it is. Nothing about the request body changes who may do it.
 *
 * <p>The reads are the opposite. They are reachable by customers, and <i>which</i> customer
 * matters - which is a question about the account id in the path, and no filter chain can answer
 * a question about a path variable it has not bound. Same distinction the orchestrator draws for
 * its POST body: a role check is not a resource check.
 */
@RestController
@RequestMapping("/accounts")
public class AccountController {

    private final AccountService accounts;

    public AccountController(AccountService accounts) {
        this.accounts = accounts;
    }

    @PostMapping
    public ResponseEntity<AccountResponse> open(@Valid @RequestBody CreateAccountRequest request) {
        Account account = accounts.open(
                request.ownerId(), request.currency(), request.openingBalanceMinor());
        return ResponseEntity
                .created(URI.create("/accounts/" + account.getId()))
                .body(AccountResponse.from(account));
    }

    /**
     * Reads one account. The caller's own, or any account if they are an operator.
     *
     * <p>Before M6 this method took no principal and returned any account to any operator, which
     * was correct while the endpoint was operator-only. Opening it to customers is what made the
     * per-resource check necessary - and the check is inside
     * {@link AccountService#getVisibleTo}, not here, so it cannot be skipped by a second caller
     * of the service.
     */
    @GetMapping("/{accountId}")
    public AccountResponse get(@PathVariable UUID accountId,
                               @AuthenticationPrincipal Jwt caller,
                               Authentication authentication) {
        return AccountResponse.from(
                accounts.getVisibleTo(accountId, caller.getSubject(), isOperator(authentication)));
    }

    /**
     * <b>M6 part 3.</b> This account's ledger entries, newest first, keyset-paged.
     *
     * <p>This is the endpoint behind the console's money view: the sender's debit, the credit into
     * CLEARING, and later the leg out of CLEARING into the recipient - the same rows that make I1
     * and I2 true, shown rather than asserted. It is also the answer to the question
     * {@code AccountOwnershipGuard} deliberately refuses on the orchestrator side: the RECIPIENT
     * of a transfer cannot read the sender's transfer record, but they can see the credit in their
     * own history, because that is a fact about their own account.
     */
    @GetMapping("/{accountId}/ledger")
    public LedgerPage ledger(@PathVariable UUID accountId,
                             @AuthenticationPrincipal Jwt caller,
                             Authentication authentication,
                             @RequestParam(required = false) String cursor,
                             @RequestParam(required = false) Integer size) {
        LedgerCursor from = cursor == null ? null : LedgerCursor.decode(cursor);
        return accounts.ledgerFor(accountId, caller.getSubject(), isOperator(authentication),
                from, size);
    }

    /**
     * Whether this token carries the operator role.
     *
     * <p>Read off the granted authorities rather than the raw {@code roles} claim, so it is the
     * same value the filter chain's {@code hasRole} rules matched on. Reading the claim directly
     * would be a second, subtly different interpretation of the token - and the prefix is exactly
     * where that goes wrong: {@code hasRole(OPERATOR)} looks for the authority
     * {@code ROLE_OPERATOR}, and a hand-rolled claim check that forgets the prefix silently
     * disagrees with every rule in {@code SecurityConfig}.
     */
    private static boolean isOperator(Authentication authentication) {
        return authentication != null && authentication.getAuthorities().stream()
                .anyMatch(a -> (Roles.PREFIX + Roles.OPERATOR).equals(a.getAuthority()));
    }
}
