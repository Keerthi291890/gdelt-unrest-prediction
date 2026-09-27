import statistics


countries = {
    "India": [
        6561, 5388, 5757, 5676, 4278,
        4273, 4721, 5969, 5710
    ],

    "Nigeria": [
        3786, 2340, 3491, 3278, 3249,
        2012, 3016, 3418, 3908
    ],

    "Brazil": [
        506, 651, 462, 437, 211,
        90, 330, 271, 348
    ],

    "Ukraine": [
        2071, 1008, 2806, 1552, 1343,
        934, 1356, 1445, 2391
    ],

    "United States": [
        44624, 25955, 30670, 46858, 16767,
        16420, 40506, 39299, 26582
    ],

    "Mexico": [
        715, 410, 564, 611, 264,
        236, 561, 580, 393
    ],

    "Indonesia": [
        611, 378, 388, 528, 244,
        194, 508, 402, 448
    ],

    "South Africa": [
        1252, 747, 995, 1322, 813,
        480, 1230, 1326, 1381
    ]
}


print("=" * 85)
print("9-MONTH COUNTRY VARIABILITY ANALYSIS")
print("=" * 85)

print(
    f"{'Country':<18}"
    f"{'Mean':>12}"
    f"{'Std Dev':>12}"
    f"{'CV':>10}"
    f"{'Min':>10}"
    f"{'Max':>10}"
)

print("-" * 85)


results = []


for country, values in countries.items():

    mean_value = statistics.mean(values)

    std_value = statistics.stdev(values)

    cv = std_value / mean_value

    minimum = min(values)

    maximum = max(values)

    results.append(
        (
            country,
            mean_value,
            std_value,
            cv,
            minimum,
            maximum
        )
    )

    print(
        f"{country:<18}"
        f"{mean_value:>12.1f}"
        f"{std_value:>12.1f}"
        f"{cv:>10.3f}"
        f"{minimum:>10}"
        f"{maximum:>10}"
    )


print("\n")
print("=" * 85)
print("RANKING BY DATA STABILITY")
print("=" * 85)

sorted_results = sorted(
    results,
    key=lambda x: x[3]
)

for rank, result in enumerate(sorted_results, 1):

    country = result[0]
    cv = result[3]

    print(
        f"{rank}. {country:<18} CV = {cv:.3f}"
    )


print("\n")
print("=" * 85)
print("PROVISIONAL TOP 5 BY VOLUME + STABILITY")
print("=" * 85)

print("""
This ranking is NOT the final research-country selection.

It is only a feasibility assessment based on:
1. GDELT ActionGeo volume
2. 9-month availability
3. Temporal variability
""")