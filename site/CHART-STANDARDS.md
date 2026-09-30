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
chart (S2). Blank cells count: they reach the color domain as values of their own, and a
JSON file's null and empty text are two (the builder counts the forms present, `blanks`).
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
measure's). A chart of part of the rows (the Total mode's totals) fits its log axis to
whole decades around those rows, not the column's full range: disasters' totals run from
about 300 to 3.7 million, so the axis starts at 100, not at 1. Such an axis labels its
decades, and draws grid lines and ticks only there (not at 2 to 9 times each decade,
which crowd the plot).

Why: a linear axis squeezes the bulk of a heavy-tailed distribution against zero and
gives the axis to a few outliers. Few recommends log scales for data spanning orders of
magnitude, labelled so that the reader sees the scale is not linear; the 1-2-5 and
power-of-ten ticks make that plain.

Tests: `explore-invariants.test.ts` (S4: log only on positive values, symlog only where
zero is in range, a heavy tail or its mean on its scale, on generated and real datasets),
`chart-standards.test.ts` (a log axis over part of the rows fits whole decades around
them; a log axis draws grid lines only where it has labels; both from the rendered view), `explore-rules.test.ts` (G-2: log and symlog axes; log ticks).

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

## S8. Lines are straight between values

A line joins its values with straight segments (Vega-Lite's default, linear), never a
smoothed curve (`monotone`, `basis`). A line with at most 60 values (`FEW_POINTS`) marks
each one.

Why: a curve through the values draws peaks and troughs that aren't in the data (crimea's
monthly deaths, iowa_electricity's yearly generation, the daily flight delays) and hides
which points were measured. Few's line graphs connect the measured points and nothing
else; with few points, the markers show the reader where the data is.

Tests: `explore-invariants.test.ts` (S8 with S1, S3 and S5, on generated and real
datasets, wide and on a phone).

## S9. A point map has a basemap

Points with coordinates always sit on a basemap. Points close together (within 10
degrees) get a Mercator map fitted to them, over the most detailed
basemap vega-datasets has that holds them: Greater London's boroughs, the United States'
counties, else the world's countries. The frame holds every point but the outliers (a
point beyond the middle 90% of the points by more than that middle's span, along either
axis), with 5% to spare on each side, so no point sits on the edge: London's outer boroughs
are the edge of the data, not outliers, and all 33 show. Outliers left out are said below
the chart, as the Albers USA map says its rows outside the 50 states (la_riots: one death
recorded 40 km east of the rest). The builder finds the
coordinate columns (`coordinate_pair`): named latitude and longitude; a centroid's `cx`
and `cy` only with geographic evidence (described as longitude and latitude or a centroid,
or every value inside a detailed basemap's box, as London's centroids are); `x` and `y`
only when their descriptions say longitude and latitude. Every value must be in range, and
values in range alone are no evidence: pixel positions stay a scatter plot.

Why: points with no map are a scatter plot with no place in it (la_riots' deaths across
Los Angeles; london_centroids drawn as x and y). The outline is what makes a position a
place.

Tests: `chart-standards.test.ts` (S9 on every real point map: a basemap, the frame's
note, and no point but an outlier outside the frame, from the data files),
`test_build_site_catalog.py` (the coordinate pair, the frame).

## S10. Labels read

No two axis labels overlap (`labelOverlap`, with room between them), no label is turned on
its side when it fits upright, date labels match the span (`dateFormat`), an integer's
histogram bins step and are labeled by whole numbers (4, not 4.0), a log or symlog
color legend labels the scale as drawn: Vega picks its ticks from the rendered domain (a
heatmap's bucket means or sums, not the raw values, whose decades may all lie outside it),
and over three decades or more only the decades are labeled. Every label lies within its
gradient, and no legend's title sits on its labels (a vertical gradient's end labels reach
half a line past its ends; the title keeps clear of them).

Why: a label that overlaps another, or has to be read sideways, is a label the reader
skips. Knaflic's advice for axes is that they be easy to read at a glance.

Tests: `browser/labels.mjs` (the rendered label and legend-title boxes of every dataset's
Explore chart in every mode, at 1360 and 390 px), `chart-standards.test.ts` and
`explore-rules.test.ts` (every rendered legend label within its gradient, on the real
charts wide and on a phone and on a heatmap whose cells span a fraction of the raw range), `chart-standards.test.ts` (integer bins, legend decades).

## S11. A time axis ends at the data

A time axis starts and ends within a tick of the data (a year axis is not `nice`: 1880 to
2023 ran to 2040). Dates past the catalog's build year are a data error, reported, not
drawn around: movies' Release Date holds films of the 1910s to 1940s dated 2015 to 2046
(two-digit years expanded into the wrong century in the source file). The metadata that
would correct it is not the site's to edit; the test lists it until the data is fixed.

Tests: `chart-standards.test.ts` (S11: the rendered domain against the data's range; the
dates past the build year).

## S12. Years on a band axis at round steps

A band axis of years (a heatmap's columns) is labeled at a round step (1, 2, 5, 10, 20,
25, 50 or 100 years) that leaves at most 12 labels, 5 on a phone: 1900, 1920, 1940, not
1906, 1911, 1916.

Tests: `chart-standards.test.ts` (S12).

## S13. A heatmap shows change over time

A heatmap of time by series colors by a rate when the table has one for the same rows
(unemployment's rate, not its count): a count colors each row by its size, so the largest
industry is darkest every month and the change over time is invisible. At least a fifth of
the color's variation must lie within rows.

Tests: `chart-standards.test.ts` (S13, from the rendered cells).

## S14. Categorical colors stay within the palette on maps too

A map colors lines or points by category only when there are at most ten (`TABLEAU10`, as
every categorical color on the site). London's 13 tube lines are one color, each named by
its tooltip: Tableau 20's paired light and dark hues can't be told apart on thin lines,
and a twelve-hue palette has the same problem at its light end.

Tests: `chart-standards.test.ts` (S14).

## S15. A time chart needs times to show

A time chart split into series (lines or heatmap rows) is drawn only when the median series
has three or more times (the builder's `perSeries`). Candidates each reporting once or
twice (political_contributions) make a grid of isolated cells, not a change over time: the
Explore modes leave the time chart out.

Tests: `chart-standards.test.ts` (S15).

## S16. No mean of a measure that is mostly zero

When more than half a measure's values are zero (the builder's `zeros`) and the rest span
decades (a log or symlog measure, S4), a bucketed time chart draws the count of records
per bucket instead of the measure's mean: most bird strikes cost nothing and a few cost
millions, so their monthly mean cost is a row of spikes; the number of strikes a month is
what the rows can show. Rain is zero on most days too, but its monthly mean (the month's
average daily rainfall) is a quantity worth a line, so the decades condition keeps it.

Tests: `chart-standards.test.ts` (S16).

## S17. Measures of one stated unit over time are drawn together

Two to six measures (four on a phone) whose metadata states one unit, all non-negative, in
a table with one row per time, are one chart: a line each, on one axis, colored by measure
(crimea's deaths from disease, wounds and other causes, Nightingale's view of the data).
A wrong fold is worse than none: one axis says the measures share a unit. So the rule
folds only on explicit evidence, the first measure's decides, and in doubt nothing folds:

- a unit in parentheses or brackets ending each title or description, the same for all
  ("(mm)", "[USD]"): "Average monthly rainfall (mm)" and "Average monthly revenue (USD)"
  don't fold, however many words they share;
- or descriptions that open with a count and a preposition, the same for all ("Deaths
  from …": each row counts deaths), for whole numbers only. Crimea's say "Deaths from
  Zymotic Diseases", "Deaths from \"Wounds and Injuries\" …" and "Deaths from All Other
  Causes"; `army_size` ("Estimated Average Monthly Strength of the Army") stays out.

Each line is keyed by its field's name, so two measures titled alike stay two series (the
legend says "Deaths (a)", "Deaths (b)"); the titles are labels only, compared as escaped
strings, never looked up by name. Each measure's documented missing values are left out
after the fold, measure by measure (a marker in one keeps the others' values on its row).
The lines break at gaps (S3), are marked when few (S8), and give way to points when a
measure is too jagged (S5).

A field Vega can't read by name is left out of every chart (`readable`): one named for an
Object property (`constructor`, `toString`, `__proto__`) breaks Vega's dataflow wherever it
is read, a backslash in a name is read as an escape however many are written, and a double
quote breaks Vega-Lite's tooltip expression. The chart draws the rest.

Tests: `chart-standards.test.ts` (S17 on the real datasets), `explore-invariants.test.ts`
(40 generated fold tables: S1, S3, S5, S8, no data loss per measure, no sum across measures,
one stated unit, series keyed by name, and unsafe names never folded, with coverage guards
for missing values, gaps, duplicate titles and names that need escaping),
`explore-rules.test.ts` (Codex round 6).
