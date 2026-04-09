4) build_sku_weekly_forecast_2_rows()도 같이 수정
def build_sku_weekly_forecast_2_rows(sku_df: pd.DataFrame, plc_df: pd.DataFrame) -> list:
    curr_year, curr_week = get_current_year_week()

    plc_df = deduplicate_item_plc(plc_df)

    actual_df = build_actual_rows(sku_df, plc_df, curr_year)
    forecast_df = build_forecast_rows(sku_df, plc_df, curr_year, curr_week)

    final_df = pd.concat([actual_df, forecast_df], ignore_index=True)

    if final_df.empty:
        return []

    final_df = final_df.sort_values(["sku", "plant", "week_no"], na_position="last").reset_index(drop=True)
    return final_df.to_dict(orient="records")
