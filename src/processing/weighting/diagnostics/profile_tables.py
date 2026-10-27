"""The report's per-profile tables: how each fit compares, and what carries weight.

One row or column per profile, so the fits can be read side by side: seed and
weight sums, the cascade of usable records, redistribution, the day split,
control-geography coverage and how lower-level weights descend from the level
above.
"""

from processing.weighting.core.specs import (
    GeographyCoverage,
)

from .comparison import Inheritance
from .data import (
    REDISTRIBUTION_TAIL,
    CascadeRow,
    ProfileSummary,
    RedistributionRow,
    SplitIdentityRow,
)
from .glossary import term
from .table_html import html_table


def profile_comparison_table(rows: list[ProfileSummary]) -> str:
    """Compare every fit in the run, one row each.

    Each profile is balanced to the same controls over its own universe, so the
    totals should land on the same targets. Divergence is the usability gate
    changing what the survey can reach, not the balancer failing -- which is why
    this table only exists once all the fits are done.
    """
    headers = [
        "Profile",
        "Households&nbsp;in&nbsp;sample",
        "&Sigma;&nbsp;hh&nbsp;weight",
        "&Sigma;&nbsp;person&nbsp;weight",
        "&Sigma;&nbsp;day&nbsp;weight",
        term("ess", "ESS&nbsp;%"),
        term("cv"),
        term("maxmed"),
        term("mape"),
        "Converged",
    ]
    body: list[list[str]] = []
    for r in rows:
        converged = f"{r.zones_converged} / {r.zones_total}"
        css = "converged" if r.zones_converged == r.zones_total else "failed"
        body.append(
            [
                r.profile or "the survey",
                f"{r.seed_households:,}",
                f"{r.weight_sums.get('households', 0.0):,.0f}",
                f"{r.weight_sums.get('persons', 0.0):,.0f}",
                f"{r.weight_sums.get('person_days', 0.0):,.0f}",
                f"{r.ess_pct:.1f}%",
                f"{r.cv:.3f}",
                f"{r.max_over_median:.1f}",
                f"{r.mape:.2f}%",
                f'<td class="{css}">{converged}</td>',
            ]
        )
    return html_table(headers, body)


def cascade_table(cascades: dict[str | None, list[CascadeRow]]) -> str:
    """Per-level weight coverage, one column group per profile.

    Shown side by side rather than behind the profile toggle: the counts are
    small enough to compare directly, and a labelled column cannot be misread
    the way a toggled chart can.
    """
    if not cascades:
        return ""
    profiles = list(cascades)
    by_table: dict[str, dict[str | None, CascadeRow]] = {}
    for profile, rows in cascades.items():
        for row in rows:
            by_table.setdefault(row.table, {})[profile] = row

    group_row = [("Level", 1), ("Rows", 1)]
    group_row += [(p or "the survey", 3) for p in profiles]
    sub_headers = ["Usable", "Weighted", "Unweighted"] * len(profiles)

    body: list[list[str]] = []
    for table, per_profile in by_table.items():
        any_row = next(iter(per_profile.values()))
        cells = [table, f"{any_row.rows:,}"]
        for profile in profiles:
            row = per_profile.get(profile)
            if row is None:
                cells += ["—", "—", "—"]
                continue
            unweighted = (
                f'<td class="failed">{row.unweighted:,}</td>'
                if row.unweighted
                else f"{row.unweighted:,}"
            )
            cells += [f"{row.usable:,}", f"{row.weighted:,}", unweighted]
        body.append(cells)
    return html_table(sub_headers, body, group_row=group_row)


def redistribution_table(rows: dict[str | None, list[RedistributionRow]]) -> str:
    """Child/parent weight ratio per copy-and-conserve edge, per profile."""
    headers = [
        "Levels",
        "Profile",
        "Median",
        "P90",
        "P99",
        "Max",
        f"&gt;&thinsp;{REDISTRIBUTION_TAIL:g}&times;",
    ]
    body: list[list[str]] = []
    for profile, edges in rows.items():
        for r in edges:
            body.append(  # noqa: PERF401
                [
                    f"{r.table} / {r.parent}",
                    profile or "the survey",
                    f"{r.p50:.2f}",
                    f"{r.p90:.2f}",
                    f"{r.p99:.2f}",
                    f"{r.maximum:.1f}",
                    f"{r.share_above:.1f}%",
                ]
            )
    return html_table(headers, body) if body else ""


def split_identity_table(rows: dict[str | None, list[SplitIdentityRow]]) -> str:
    """Whether each split level's children still sum to their parent's weight."""
    headers = [
        "Levels",
        "Profile",
        "Higher-level&nbsp;records",
        "Largest&nbsp;gap",
        "With&nbsp;no&nbsp;usable&nbsp;lower-level&nbsp;record",
        "Weight&nbsp;not&nbsp;passed&nbsp;down",
        "Lower-level&nbsp;records&nbsp;each",
    ]
    body: list[list[str]] = []
    for profile, edges in rows.items():
        for r in edges:
            stranded = (
                f'<td class="failed">{r.stranded_parents:,}</td>'
                if r.stranded_parents
                else f'<td class="converged">{r.stranded_parents:,}</td>'
            )
            body.append(
                [
                    f"{r.table} / {r.parent}",
                    profile or "the survey",
                    f"{r.parents_checked:,}",
                    f"{r.max_residual:.1e}",
                    stranded,
                    f"{r.stranded_weight:,.0f}",
                    f"{r.min_children}&ndash;{r.max_children} (med {r.median_children})",
                ]
            )
    return html_table(headers, body) if body else ""


def coverage_table(coverages: dict[str | None, GeographyCoverage]) -> str:
    """Households each profile admits that the control geography cannot place.

    A household outside every control polygon belongs to no balancing zone, so
    no fit can give it a weight. That is a bound on what the weighting answers,
    and it is stated rather than left to be inferred from a column of nulls.
    """
    headers = [
        "Profile",
        "Households",
        "In&nbsp;a&nbsp;zone",
        "Outside&nbsp;all&nbsp;zones",
        "Share",
    ]
    body: list[list[str]] = []
    for profile, cov in coverages.items():
        unplaceable = (
            f'<td class="failed">{cov.n_unplaceable:,}</td>'
            if cov.n_unplaceable
            else f'<td class="converged">{cov.n_unplaceable:,}</td>'
        )
        body.append(
            [
                profile or "the survey",
                f"{cov.n_universe:,}",
                f"{cov.n_placed:,}",
                unplaceable,
                f"{cov.unplaceable_share * 100:.2f}%",
            ]
        )
    return html_table(headers, body)


def inheritance_table(rows: list[Inheritance], labels: dict[str, str]) -> str:
    """How each set's weight descends from the level above, and by what mechanism.

    The mechanism is the column that matters. *Copied* means the level adds
    nothing of its own, so a comparison there restates the level above. *Adjusted*
    means records were rescaled individually after the weight descended, which is
    the one case where the level no longer nests inside its parent -- and the one
    a reader is most likely to mistake for the harmless kind.
    """
    if not rows:
        return ""
    headers = [
        "Level",
        "Weight set",
        "Records",
        term("inherit", "Same&nbsp;as&nbsp;level&nbsp;above"),
        "Groups&nbsp;with&nbsp;unequal&nbsp;adjustments",
        "Largest&nbsp;difference",
        "Mechanism",
    ]
    verdicts = {
        "copied": '<td class="failed">copied</td>',
        "redistributed": "redistributed",
        "adjusted": '<td class="failed">adjusted per record</td>',
    }
    body: list[list[str]] = []
    for row in rows:
        uniform = row.max_scope_spread <= 1 + 1e-9
        spread = "&mdash;" if uniform else f"{row.max_scope_spread:,.2f}&times;"
        varying = (
            f'<td class="failed">{row.n_scopes_varying:,}</td>'
            if row.n_scopes_varying
            else f"{row.n_scopes_varying:,}"
        )
        body.append(
            [
                f"{row.level} &larr; {row.parent}",
                labels.get(row.set_name, row.set_name),
                f"{row.n:,}",
                f"{row.share:.1f}%",
                varying,
                spread,
                verdicts[row.mode],
            ]
        )
    return html_table(headers, body, css_class="inherit")
