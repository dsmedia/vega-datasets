# Chart standards

The Field Guide draws a chart for every dataset from its metadata and a profile of its
data, never from a per-dataset override. The rules that choose the chart live in
`src/lib/chart-rules.ts` and `src/lib/starter.ts` (G-1 to G-7, DOSSIER §6.1). This file
states the visual standards those rules must meet, why, and which test holds each one.
A rule change that breaks a standard fails a test; a standard changes here first.

The sources are the usual ones for statistical charts: Stephen Few, *Show Me the
Numbers* (2nd ed., 2012) and *Now You See It* (2009); Cole Nussbaumer Knaflic,
*Storytelling with Data* (2015); and the [Vega-Lite example gallery](https://vega.github.io/vega-lite/examples/),
whose charts the Field Guide's readers go on to adapt.

## S1. At most six colored lines, four on a phone

A line chart colors at most six series on a wide screen and at most four on a phone
(`SERIES_LIMIT` in `chart-rules.ts`). With more, the chart is a heatmap instead: time
across, one row per series, the measure as a sequential color (Vega-Lite's `blues`),
on a log scale when the measure's positive values span three decades or more (S4). A
series value that is the total of the others is left out of the count and out of the
chart (S2). Empty cells count: they reach the color domain as a value of their own.
The choice depends only on the data: the same table always gets the same chart.

Why: telling lines apart by hue alone works for a handful of them. Few puts the limit
for categorical color at about eight hues that stay distinct, fewer for thin lines that
cross; Knaflic advises against more than a few series in one line chart and suggests
small multiples or emphasis instead. A phone's legend, above the plot, has room for a
row or two, and its lines are drawn thinner and closer together. A heatmap keeps every
series visible and comparable without asking the reader to match colors, as in
Vega-Lite's "Annual Weather Heatmap" and "Lasagna Plot" examples.

Tests: `explore-invariants.test.ts` (chart standards: S1 on every generated table and
every real dataset, wide and on a phone, from the rows as drawn), `explore-rules.test.ts`
(series colors; Codex round 5, #3 for empty cells).

## S2. A total never shares axes with its parts

A category value that is the sum of the others (the builder's `totalValues`: "All
natural disasters", "World", "Total") is not drawn with the parts. The parts' chart
filters it out; a mode of its own, **Total**, draws the total alone. No row is removed
from the data: every row is in one mode or the other.

Why: a total on the same axis as its parts compresses them against zero (it is, by
definition, the largest), and a reader summing the lines counts everything twice. Few's
guidance on part-to-whole relationships is to show the whole separately or as a stack,
never as one more peer.

Tests: `explore-invariants.test.ts` (S2 on generated and real datasets), `explore-rules.test.ts`
(the Total mode).

## S3. Lines break at gaps

A line crosses no gap in its times. The builder records each time field's usual step
and the widest gap a line may cross (`timeSteps`: 1.5 times the 90th percentile of the
gaps between neighbouring times, so a weekend in daily trading data is crossed and a
missing decade is not), and whether some series skips there (`lineShapes`). Where one
does, the line splits into segments at each gap (a `lag` window and a running segment
number, as `detail`), with points marking the times. Bucketed lines (a time unit) that
skip a bucket draw as points instead. This holds for every line chart, not only those
the rules picked for a gap: the test computes the gaps from the rows.

Why: a line asserts continuity. Drawn across a gap, it invents values nobody measured;
Few's line graphs are for values measured at regular intervals, and Vega-Lite itself
breaks a line at a null value for the same reason.

Tests: `explore-invariants.test.ts` (S3 on generated and real datasets, from the rows).

## S4. A heavy tail gets a log axis

A measure whose positive values span three decades or more, or two with over a third
of them in the histogram's first bin, is drawn on a log scale (`scaleType` in
`chart-rules.ts`): its axis ticks are 1-2-5 steps up to three decades, powers of ten
beyond. A measure with zeros or negatives can't be log; it gets `symlog` under a
stricter test (three decades and piled up near zero), and so does a positive measure
whose documented range reaches zero. A mean (a line of monthly averages) and a
heatmap's color keep the measure's scale; a sum does not (its range is not the
measure's).

Why: a linear axis squeezes the bulk of a heavy-tailed distribution against zero and
gives the axis to a few outliers. Few recommends log scales for data spanning orders of
magnitude, labelled so that the reader sees the scale is not linear; the 1-2-5 and
power-of-ten ticks make that plain.

Tests: `explore-invariants.test.ts` (S4: log only on positive values, symlog only where
zero is in range, a heavy tail drawn unaggregated on its scale, on generated and real
datasets), `explore-rules.test.ts` (G-2: log and symlog axes; log ticks).

## S5. A jagged series is points, not a line

When a measure jumps too much from one time to the next for a line to read (the median
absolute step within a series, over the measure's range, above `JAGGED` = 0.2, on the
scale it is drawn with), the chart draws points. The builder measures this jaggedness
per time, measure and way of splitting the rows (`lineShapes`), bucketed as the chart
buckets them.

Why: a line through values that swing across most of the range at every step is a
scribble; the eye follows the line's slope, which there carries no information. Points
show the same values and their spread, as a scatter plot of the measure against time
(Few and Knaflic both reserve lines for values whose change from one point to the next
means something).

Tests: `explore-invariants.test.ts` (S5 on generated and real datasets: every line's
jaggedness from the rows is at most `JAGGED`; points come up for jagged series).

## S6. Nothing drawn past the chart's column on a phone

On a phone (390 px, and the narrowest in use, 320 px: a 288 px column), no chart draws
past its column. A legend sits above the plot, in as many columns as fit at its longest
label's width, sized from the labels it shows (documented labels where the metadata
has them, not the codes). Small multiples are 100 px panels, two to a row. A heatmap's
row labels are cut at 100 px (the tooltip gives them in full), so the cells and the
legend keep the rest.

Why: a chart that overflows is clipped or scrolls sideways, and the part out of view is
usually the legend, which the reader needs to read the chart at all.

Tests: `phone-legends.test.ts` (every colored chart at 358 and 288 px: the legend takes
no width from the plot and nothing is drawn past the chart's edge), `browser/overflow.mjs`
(every page at 320, 390 and 1360 px: no horizontal scroll, and the drawn Explore chart
stays inside its column on phones).

## S7. A heatmap's time axis reads as time

A heatmap of time by series (S1) puts buckets of a time unit on a time axis: each cell
as wide as its bucket, ticks and labels as a line chart's (`dateFormat`), not a label per
column. A date the line chart would draw as it is (a small table) keeps its dates: a
column per day while there are at most 80 distinct dates, in order, else per year.

Why: the first contact sheet showed both failures. A decade of months as 120 ordinal
columns crowded its labels into one run of text ("Jan 2000Nov 2000Sep 2001…"), and a
year of twenty report dates, bucketed by year, collapsed into a single column (a "time"
chart with one time). Vega-Lite's own heatmaps of dates ("Annual Weather Heatmap") bucket
by a unit on a time scale.

Tests: `explore-rules.test.ts` (Visual standards review: S7).
