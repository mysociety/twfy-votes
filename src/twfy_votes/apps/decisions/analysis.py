import pandas as pd
from tqdm import tqdm


def get_commons_clusters(df: pd.DataFrame, quiet: bool = True) -> pd.Series:  # type: ignore
    """
    Cluster analysis in a box - expects the following columns to be present:
    opp_aye	opp_no gov_aye gov_no other
    Will return a series of column labels.
    As these are not normalised values, these should stay accurate descriptions.
    """

    center_df = pd.DataFrame(
        {
            "Gov rejects, strong opp (M)": {
                "opp_aye": 210.1997354497351,
                "opp_no": 4.456349206349387,
                "gov_aye": 1.7923280423282648,
                "gov_no": 288.8716931216933,
                "other": 144.67989417989403,
            },
            "Gov proposes, strong opp": {
                "opp_aye": 5.066371681415276,
                "opp_no": 230.0387168141586,
                "gov_aye": 298.6006637168138,
                "gov_no": 4.487831858407759,
                "other": 111.80641592920398,
            },
            "Opp proposes, low participation": {
                "opp_aye": 206.66176470588238,
                "opp_no": 2.4558823529412024,
                "gov_aye": 24.999999999999986,
                "gov_no": 25.764705882353013,
                "other": 390.1176470588234,
            },
            "Gov rejects, weak opp": {
                "opp_aye": 44.77178423236509,
                "opp_no": 31.207468879668028,
                "gov_aye": 6.03734439834048,
                "gov_no": 277.4522821576765,
                "other": 290.53112033195004,
            },
            "Gov proposes, weak opp": {
                "opp_aye": 12.960474308300206,
                "opp_no": 43.873517786561315,
                "gov_aye": 273.2687747035573,
                "gov_no": 3.573122529644138,
                "other": 316.3241106719366,
            },
            "Low participation": {
                "opp_aye": 21.425287356321746,
                "opp_no": 27.505747126436738,
                "gov_aye": 24.626436781609343,
                "gov_no": 32.33333333333347,
                "other": 544.1091954022993,
            },
            "Gov rejects, strong opp (H)": {
                "opp_aye": 260.65412186379876,
                "opp_no": 6.274193548387416,
                "gov_aye": 2.2186379928315034,
                "gov_no": 313.8440860215053,
                "other": 67.00896057347714,
            },
            "Bipartisan support": {
                "opp_aye": 198.49019607843144,
                "opp_no": 23.686274509803944,
                "gov_aye": 259.6568627450981,
                "gov_no": 27.735294117647157,
                "other": 140.43137254901967,
            },
        }
    ).transpose()  # type: ignore

    clusters: list[str] = []

    required_columns = list(center_df.columns)
    if len([x for x in df.columns if x in required_columns]) < 5:
        raise ValueError("Dataframe missnig all required columns")
    tdf = list(df[required_columns].transpose().items())  # type: ignore
    for _, series in tqdm(tdf, total=len(tdf), disable=quiet):  # type: ignore
        value: str = (
            (center_df - series).pow(2).sum(axis=1).pow(1.0 / 2).sort_values().index[0]  # type: ignore
        )
        clusters.append(value)  # type: ignore

    return pd.Series(clusters, index=df.index)  # type: ignore


def is_nonaction_vote(motion_text: str, quiet: bool = True) -> bool:
    """
    Analyse the text of a motion to determine if it is a non-action motion
    """
    non_action_phrases = [
        "believes",
        "regrets",
        "notes with approval",
        "expressed approval",
        "welcomes",
        "is concerned",
        "calls on the",
        "recognises",
        "takes note",
        "agrees with the goverment's decision",
    ]
    action_phrases = [
        "orders that",
        "requires the Goverment",
        "censures",
        "declines to give a Second Reading",
    ]

    # this doesn't seem like a thing
    # commits the Government

    reduced_text = motion_text.lower()

    score = 0
    for phrase in non_action_phrases:
        if phrase in reduced_text:
            if not quiet:
                print(f"matched {phrase}")
            score += 1

    for phrase in action_phrases:
        if phrase in reduced_text:
            if not quiet:
                print(f"matched {phrase}- is action")
            score = 0

    return score > 0
