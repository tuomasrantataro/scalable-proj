package scalable.flink.ema;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Which traders subscribe to which symbols (FR-4: advisories are delivered only for subscribed symbols).
 * Read once at job start from a text file, one trader per line:
 *
 * <pre>
 *   # comment
 *   trader-paris: TTE.FR, PX1.FR ALNEV.FR     symbols separated by commas and/or whitespace
 *   trader-all:   *                           every symbol
 * </pre>
 *
 * A trader may appear on several lines; the symbols are combined. Malformed lines are errors, so a typo cannot
 * silently drop a trader's subscriptions.
 */
public final class Subscriptions implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Pattern TRADER = Pattern.compile("[A-Za-z0-9_.-]+");
    private static final Pattern SYMBOL = Pattern.compile("[A-Za-z0-9_.-]+");

    /** symbol → traders subscribed to it, sorted (traders with '*' excluded). */
    private final Map<String, List<String>> bySymbol;
    /** Traders subscribed to every symbol, sorted. */
    private final List<String> allSymbols;
    private final int traders;
    private final int subscriptions;

    private Subscriptions(Map<String, List<String>> bySymbol, List<String> allSymbols, int traders, int subscriptions) {
        this.bySymbol = bySymbol;
        this.allSymbols = allSymbols;
        this.traders = traders;
        this.subscriptions = subscriptions;
    }

    public static Subscriptions load(Path file) throws IOException {
        return parse(Files.readAllLines(file, StandardCharsets.UTF_8), file.toString());
    }

    public static Subscriptions parse(List<String> lines, String source) {
        Map<String, TreeSet<String>> symbolTraders = new HashMap<>();
        TreeSet<String> wildcard = new TreeSet<>();
        TreeSet<String> traders = new TreeSet<>();
        int subscriptions = 0;
        for (int n = 0; n < lines.size(); n++) {
            String line = lines.get(n);
            int hash = line.indexOf('#');
            if (hash >= 0) {
                line = line.substring(0, hash);
            }
            line = line.strip();
            if (line.isEmpty()) {
                continue;
            }
            int colon = line.indexOf(':');
            String where = source + ":" + (n + 1);
            if (colon < 0) {
                throw new IllegalArgumentException(where + ": expected 'trader: SYMBOL ...', got '" + line + "'");
            }
            String trader = line.substring(0, colon).strip();
            if (!TRADER.matcher(trader).matches()) {
                throw new IllegalArgumentException(where + ": invalid trader name '" + trader + "'");
            }
            traders.add(trader);
            for (String sym : line.substring(colon + 1).split("[,\\s]+")) {
                if (sym.isEmpty()) {
                    continue;
                }
                if (sym.equals("*")) {
                    if (wildcard.add(trader)) {
                        subscriptions++;
                    }
                } else if (SYMBOL.matcher(sym).matches()) {
                    if (symbolTraders.computeIfAbsent(sym, k -> new TreeSet<>()).add(trader)) {
                        subscriptions++;
                    }
                } else {
                    throw new IllegalArgumentException(where + ": invalid symbol '" + sym + "'");
                }
            }
        }
        Map<String, List<String>> bySymbol = new HashMap<>();
        symbolTraders.forEach((sym, ts) -> {
            ts.removeAll(wildcard); // already covered by '*'
            if (!ts.isEmpty()) {
                bySymbol.put(sym, List.copyOf(ts));
            }
        });
        return new Subscriptions(bySymbol, List.copyOf(wildcard), traders.size(), subscriptions);
    }

    /** Traders that receive advisories for {@code symbol}, sorted; empty if nobody subscribes. */
    public List<String> tradersOf(String symbol) {
        List<String> named = bySymbol.getOrDefault(symbol, Collections.emptyList());
        if (allSymbols.isEmpty()) {
            return named;
        }
        if (named.isEmpty()) {
            return allSymbols;
        }
        // disjoint: '*' traders were removed from the named lists
        List<String> both = new ArrayList<>(named.size() + allSymbols.size());
        both.addAll(named);
        both.addAll(allSymbols);
        Collections.sort(both);
        return both;
    }

    public int traderCount() {
        return traders;
    }

    /** Number of (trader, symbol) pairs, counting '*' once per trader. */
    public int subscriptionCount() {
        return subscriptions;
    }

    public int symbolCount() {
        return bySymbol.size();
    }

    public int wildcardTraderCount() {
        return allSymbols.size();
    }

    @Override
    public String toString() {
        return String.format("%d traders, %d subscriptions, %d named symbols, %d traders with '*'",
                traders, subscriptions, bySymbol.size(), allSymbols.size());
    }
}
