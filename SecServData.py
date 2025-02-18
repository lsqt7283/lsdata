# -*- coding: utf-8 -*-
"""
Created on Wed Mar 10 11:39:06 2021

Run Data Query via Security Service REST API

## Documentation of API: http://in2apps/securityservice/query-securities-data

@author: TQiu
"""


import pandas as pd
import datetime


def isValidSecServFields(cols):
    """
    Inputs>>
    cols: comma delimitted list of valid fields (as per API documentation)
    Outputs>>
    boolean: indicator if fields are all valid
    """
    valid_cols = 'returnDays001, abs_msaa_sector, abstrat_derivative, abstrat_emerging, abstrat_id, abstrat_industry, abstrat_rating, abstrat_region_of_risk, abstrat_wcaf, accrued_interest, adjusted_key_rate_duration_beta_100_day_five_year, adjusted_key_rate_duration_beta_100_day_one_year, adjusted_key_rate_duration_beta_100_day_seven_year, adjusted_key_rate_duration_beta_100_day_six_months, adjusted_key_rate_duration_beta_100_day_ten_year, adjusted_key_rate_duration_beta_100_day_thirty_year, adjusted_key_rate_duration_beta_100_day_three_year, adjusted_key_rate_duration_beta_100_day_total, adjusted_key_rate_duration_beta_100_day_twenty_year, adjusted_key_rate_duration_beta_100_day_two_year, adjusted_key_rate_duration_beta_five_year, adjusted_key_rate_duration_beta_one_year, adjusted_key_rate_duration_beta_seven_year, adjusted_key_rate_duration_beta_six_months, adjusted_key_rate_duration_beta_ten_year, adjusted_key_rate_duration_beta_three_year, adjusted_key_rate_duration_beta_thrity_year, adjusted_key_rate_duration_beta_total, adjusted_key_rate_duration_beta_twenty_year, adjusted_key_rate_duration_beta_two_year, adjusted_key_rate_duration_five_year, adjusted_key_rate_duration_one_year, adjusted_key_rate_duration_seven_year, adjusted_key_rate_duration_six_months, adjusted_key_rate_duration_ten_year, adjusted_key_rate_duration_thirty_year, adjusted_key_rate_duration_three_year, adjusted_key_rate_duration_twenty_year, adjusted_key_rate_duration_two_year, adr_sh_per_adr, agency_group, aifm_flag, alias_id, alpha_strategy_scheme_1, amount_outstanding, analyst, analyst_first_name, analyst_group_desc, analyst_group_id, analyst_initials, analyst_last_name, analytic_exception_flag, analytic_id, appraisal_type_name, as_of_date, ask_price, asset_class, australian_sanction_status, avg_volume_30day, bank_loan_rating, bank_loan_reissue_flag, bankloan_libor_floor, bankloan_libor_spread, bankloan_std_loan_desc, bankloan_std_loan_desc_code, bart_cluster_industry_dts_10_20_factor, bart_cluster_industry_dts_10_20_name, bart_cluster_industry_rating_factor, bart_cluster_industry_rating_name, base_12month_excess_return, base_12month_return, bb_bl_id, bb_comp_cd, bb_crp_eq_tk, bb_gl_cd, bb_shrt_name, bb_uniq_cd, bid_price, bloomberg_industry_subgroup_code, bloomberg_industry_subgroup_name, braeburn_price, bric, brics, bval_price, cadis_issuer_id, calculated_g_spread, calculated_g_spread_latest, calculated_roll_spread_net_residual_spread, calculated_roll_spread_net_residual_spread_delta_30_day, calculation_unit, call_announcement_date, call_type, callable, canadian_sanction_status, cap_structure_type, carbon_emiss_intensity_year, carbon_emiss_scope_1, carbon_emiss_scope_12, carbon_emiss_scope_12_inten, carbon_emiss_scope_12_key, carbon_emiss_scope_1_key, carbon_emiss_scope_2, carbon_emiss_scope_2_key, carbon_emiss_scope_3, carbon_emiss_scp_12_ind_inten, carbon_emiss_scp_12_inten_3yav, carbon_emiss_year, carbon_sales_intensity_recent, cdx_type, citi_level_four_code, citi_level_four_name, citi_level_one_code, citi_level_one_name, citi_level_three_code, citi_level_three_name, citi_level_two_code, citi_level_two_name, clean_price_local_currency, coal_potential_emissions, collateral, collateral_net_coupon, comp_modified_duration_to_worst, comp_spread_delta_1day, comp_spread_delta_30day, comp_spread_delta_90day, comp_spread_delta_mtd, comp_spread_delta_qtd, comp_spread_delta_ytd, comp_spread_percent_change_1day, comp_spread_percent_change_30day, comp_spread_percent_change_90day, comp_spread_percent_change_mtd, comp_spread_percent_change_qtd, comp_spread_percent_change_ytd, comp_yield_source_name, company_corporate_ticker, company_name, conv_oil_analyst_comments, conversion_percent_premium, convert_gamma, convert_gamma_per_share, convert_sensitivity_type, convertible_preferred_scheme, convexity, corp_credit_acct_reserves, country_code, country_description, country_of_domicile, country_of_domicile_name, country_of_risk, country_of_risk_name, coupon_band, coupon_frequency, coupon_rate, coupon_type, cr_analyst_id, cr_base_case, cr_best_case, cr_downside_case, cr_first_name, cr_initials, cr_last_name, cr_target_date, cr_worst_case, credit_bucket, credit_bucket_ex_nr, credit_scope_score, cs_01, currency_code, currency_code_modified_for_options, currency_country_issuance, currency_desc, currency_description, currency_forward, currency_forward_offset, currency_name_modified_for_options, currency_type, current_coupon_spread_sesitivity, current_dividend_yield, current_yield, current_yield_bands, curropt_flag, cusip, cusip_9, custom_barclays_securitized_level_four, custom_barclays_securitized_level_one, custom_barclays_securitized_level_three, custom_barclays_securitized_level_two, custom_cluster_code, custom_cluster_name, custom_cluster_seniority, custom_cluster_ultimate_parent_company_name, custom_maif, custom_securitized_duration, cv_01, date_reviewed, dated_date, day_count_method, default_security_id, delta_clean, delta_model_s, derivative, derivative_offset, developed_em_marketing, dirty_price_local_currency, div_yld_12months, dividend_paid, dly_return, downgrade_risk, downside_12month_excess_return, downside_12month_return, duration, dv_01, ebitda_operations_annual, ebitda_operations_annual_date, ebitda_operations_quarter, ebitda_operations_quarter_date, effective_duration, eiopa_down_delta, eiopa_up_delta, em_custom_fitch_numeric_rating, em_custom_fitch_rating, em_custom_industry_level_code, em_custom_industry_level_name, em_custom_moody_numeric_rating, em_custom_moody_rating, em_custom_standard_poors_numeric_rating, em_custom_standard_poors_rating, em_team_country_of_risk, em_team_country_of_risk_name, emerging_market_currency_region, emerging_market_id, emerging_market_level_five_scheme, emerging_market_level_four_scheme, emerging_market_level_one_scheme, emerging_market_level_six_scheme, emerging_market_level_three_code, emerging_market_level_three_scheme, emerging_market_level_two_scheme, empirical_duration, empirical_duration_rsquared, empirical_key_rate_duration_five_year, empirical_key_rate_duration_offset, empirical_key_rate_duration_six_month, empirical_key_rate_duration_ten_year, empirical_key_rate_duration_thirty_year, empirical_key_rate_duration_twenty_year, empirical_key_rate_duration_two_year, equity_rep_sec_id, erisa_eligible, estimated_price_earnings_one_year, euro_credit_duration_times_spread, euro_credit_sector_mapping, european_union_sanction_status, expiration_date, face_value_factor, facility_code, facility_name, fas157_price_rank, fd_adjusted_ticker, fd_best_idea_1, fd_country_1, fd_country_2, fd_country_of_risk_1, fd_country_of_risk_2, fd_cpfd_bi_target, fd_currency_1, fd_custom_1, fd_custom_2, fd_custom_3, fd_custom_4, fd_custom_theme_1, fd_custom_theme_2, fd_custom_theme_3, fd_custom_theme_4, fd_es_b, fd_hycv_bi_target, fd_hyfd_bi_target, fd_irr_bucket, fd_marketing_1, fd_marketing_2, fd_marketing_test_1, fd_marketing_test_2, fd_msfd_bi_target, fd_msfda_bi_target, fd_securitized_theme, fd_test_1, fd_test_2, fd_test_3, fd_test_4, fd_theme_type, fds_perm_id, fe_est_3_5_yr_eps_mean_lt_growth, fe_forward_pe, fe_trailing_pe, fed_support, ff_assets_annual, ff_capex_pct_sales_annual, ff_cash_from_operations_annual, ff_cash_qtrly, ff_debt_annual, ff_debt_qtrly, ff_debt_to_capital, ff_debt_to_capital_ds, ff_div_yld_last_div_times_4_plus_spec, ff_dividend_payout_ratio_annual, ff_dividends_per_share_annual, ff_ebit_margin_annual, ff_ebitda_margin_annual, ff_enterprise_value_qtrly, ff_eps_annual, ff_eps_ltm, ff_equity_annual, ff_fcf_per_share_annual, ff_fcf_yield_ds, ff_free_cash_flow_annual, ff_free_cash_flow_per_share_ltm, ff_gross_profit_margin_annual, ff_industry, ff_lt_debt_to_capital_annual, ff_market_cap_annual, ff_market_cap_daily, ff_market_cap_millions_qtrly, ff_name, ff_net_income_annual, ff_p_to_e_daily, ff_p_to_e_qtrly, ff_price_to_book_daily, ff_price_to_book_ds, ff_price_to_book_value_qtrly, ff_price_to_cash_flow, ff_price_to_cash_flow_annual, ff_price_to_cash_flow_qtrly, ff_price_to_cashflow_ds, ff_price_to_sales, ff_price_to_sales_annual, ff_price_to_sales_qtrly, ff_return_on_assets_annual, ff_roe_annual, ff_roe_ds, ff_sales_growth_annual, ff_shares_qtrly, ff_split_adj_price_daily, ff_tax_rate_annual, ff_ticker, ff_total_debt_to_equity, ff_total_debt_to_equity_annual, ff_total_debt_to_equity_qtrly, ff_totl_div_annual, first_coupon_date, fitch_only_split_rating, fitch_only_split_rating_band_name, fitch_only_split_rating_mapped, fitch_outlook, fitch_rating, fitch_underlying_rating, fitch_watch, floating_coupon_type, full_discretion_four_theme, full_discretion_id, full_discretion_one_theme, full_discretion_three_theme, full_discretion_two_theme, full_inventory_market_value, full_inventory_quantity, full_inventory_weight, fv_oas_term_matched, fv_oas_term_matched_lgd_60, fv_spread, fx_rate, g4_em_other, gei_region_desc, geo_analyst_id, geo_base_case, geo_best_case, geo_downside_case, geo_first_name, geo_initials, geo_last_name, geo_target_date, geo_worst_case, gic, gics_level_four_code, gics_level_four_name, gics_level_one_code, gics_level_one_name, gics_level_three_code, gics_level_three_name, gics_level_two_code, gics_level_two_name, gics_mapped, global_barclays_level_four, global_barclays_level_one, global_barclays_level_three, global_barclays_level_two, global_corp_offshore_proxy_currency, global_credit_offshore_proxy_currency, global_currency_code, global_factor_attribution_scheme, global_growth_country_of_risk, global_growth_country_of_risk_name, global_growth_region_of_risk, global_id, global_issuer_code, global_level_one_scheme, global_level_three_scheme, global_level_two_scheme, global_obligor_company_bb_id, global_opportunistic_offshore_proxy_currency, global_region_of_risk, global_team_currency_allocation, gps_sec_id, grantor_trust_flag, green_bond_loan_ind, hedge_cost_adjusted_yield_aud_1m, hedge_cost_adjusted_yield_aud_3m, hedge_cost_adjusted_yield_cad_1m, hedge_cost_adjusted_yield_cad_3m, hedge_cost_adjusted_yield_eur_1m, hedge_cost_adjusted_yield_eur_3m, hedge_cost_adjusted_yield_gbp_1m, hedge_cost_adjusted_yield_gbp_3m, hedge_cost_adjusted_yield_jpy_1m, hedge_cost_adjusted_yield_jpy_3m, hedge_cost_adjusted_yield_nok_1m, hedge_cost_adjusted_yield_nok_3m, hedge_cost_adjusted_yield_nzd_1m, hedge_cost_adjusted_yield_nzd_3m, hedge_cost_adjusted_yield_sgd_1m, hedge_cost_adjusted_yield_sgd_3m, hedge_cost_adjusted_yield_usd_1m, hedge_cost_adjusted_yield_usd_3m, her_magestys_treasury_sanction_status, high_vol_flag, hong_kong_sanction_status, iboxx_level_four_name, iboxx_level_one_name, iboxx_level_three_name, iboxx_level_two_name, iboxx_libor_zv_spread, iboxx_oa_spread_dur, iboxx_zv_spread, iboxx_zv_spread_delta_180day, iboxx_zv_spread_delta_1day, iboxx_zv_spread_delta_30day, iboxx_zv_spread_delta_60day, iboxx_zv_spread_delta_7day, iboxx_zv_spread_delta_90day, iboxx_zv_spread_delta_mtd, iboxx_zv_spread_delta_qtd, iboxx_zv_spread_delta_ytd, iboxx_zv_spread_percent_change_180day, iboxx_zv_spread_percent_change_1day, iboxx_zv_spread_percent_change_30day, iboxx_zv_spread_percent_change_60day, iboxx_zv_spread_percent_change_7day, iboxx_zv_spread_percent_change_90day, iboxx_zv_spread_percent_change_mtd, iboxx_zv_spread_percent_change_qtd, iboxx_zv_spread_percent_change_ytd, ice_liq_scr_v_all_bnds, ice_liq_scr_v_same_asset_class, ice_liq_scr_v_same_iss, ice_liq_scr_v_same_sector, ice_lq_scr_v_sim_amt_out_bnd_a_cls, ice_lq_scr_v_sim_dur_bnd_a_cls, ice_lq_scr_v_sim_yld_mat_bnd_a_cls, id, id_bb_company, id_bb_global_company, id_bb_global_company_name, id_bb_global_obligor_company_name, id_bb_parent_co, id_bb_ultimate_parent_co, ig_hy_lehman, illiquid, impairment_risk, implied_mbs_coupon_swap_convexity, implied_mbs_coupon_swap_duration, industry_group, industry_level_id, industry_model_code, industry_model_id, industry_outlook, industry_sector, industry_subgroup, inflation_linked_indicator, insert_timestamp, instrument_type, instrument_type_name, international_securitized, inventory_type_name, inverted_face_value_factor, is_perpetual, is_reg_s, is_tba_cash, isin, iss_peq_tk, issuance, issue_amount, issue_date, issue_price, issue_type, issue_year, issuer_code, issuer_comment, issuer_industry, issuer_name, issuer_name_types, jp_asia_credit_industry_level_code, jp_asia_credit_industry_level_name, jp_embi_industry_level_code, jp_embi_industry_level_name, jp_industry_name, last_research_date, lcc_theme_tag, lec_seniority_1, lec_seniority_2, legal_entity_identifier, level_1_code, level_1_name, level_2_code, level_2_name, level_3_code, level_3_name, level_4_code, level_4_name, lg_analysty_id, lg_base_case, lg_best_case, lg_downside_case, lg_first_name, lg_initials, lg_last_name, lg_target_date, lg_worst_case, libor_option_adjusted_convexity, libor_option_adjusted_duration, libor_option_adjusted_spread, libor_option_adjusted_spread_duration, libor_zero_volatility_spread, limited_partnership, liquidity_cost_score, loan_part_note, london_inventory, loomis_expected_rating_change, loomis_fund, loomis_grade, loomis_industry_level_four_name, loomis_quality_only_split_rating, loomis_quality_only_split_rating_band_name, loomis_quality_rating, loomis_rating, loomis_research_rating, loomis_risk_rating, loomis_split_rating, loomis_split_rating_band_name, loomis_split_rating_mapped, ls_prop_collateral, ls_prop_collateral_net_coupon, ls_prop_current_coupon_spread_sensitivity, ls_prop_discount_margin, ls_prop_duration, ls_prop_effective_convexity, ls_prop_effective_duration, ls_prop_effectived_yield, ls_prop_libor_option_adjusted_spread, ls_prop_option_adjusted_spread, ls_prop_origination_year, ls_prop_partial_duration_02_year, ls_prop_partial_duration_05_year, ls_prop_partial_duration_07_year, ls_prop_partial_duration_10_year, ls_prop_partial_duration_20_year, ls_prop_partial_duration_30_year, ls_prop_partial_duration_custom, ls_prop_prepay_duration, ls_prop_price, ls_prop_spread_covexity, ls_prop_spread_delta_7day, ls_prop_spread_duration, ls_prop_spread_duration_plus_coupon_sensitivity, ls_prop_spread_percent_change_7day, ls_prop_spread_times_spread_duration, ls_prop_ticker, ls_prop_volatility_duration, ls_prop_weighted_average_life, ls_prop_yield_to_maturity, ls_prop_yield_to_worst, ls_prop_zero_volatility_spread, lt_outlook, lv_analyst_id, lv_base_case, lv_best_case, lv_downside_case, lv_first_name, lv_initials, lv_last_name, lv_target_date, lv_worst_case, macauly_duration_to_worst, mandatory_conversion_indicator, marketing_maturity_detail_ytm, marketing_maturity_summary_ytm, marketing_median_split_rating_band_name, marketing_median_split_rating_mapped, marp_asset_class, maturity_date, median_factor_rating, merrill_composite_rating, merrill_level_four_code, merrill_level_four_name, merrill_level_one_code, merrill_level_one_name, merrill_level_three_code, merrill_level_three_name, merrill_level_two_code, merrill_level_two_name, met_coal_analyst_comments, met_coal_reserves_volume, mifid_reference_index_name, min_increment, min_piece, mktg_industry_level_four_name, mktg_industry_level_one_name, mktg_industry_level_three_name, mktg_industry_level_two_name, modified_duration_to_worst, moody_cfr_rating, moody_cfr_watch, moody_only_split_rating, moody_only_split_rating_band_name, moody_only_split_rating_mapped, moody_outlook, moody_rating, moody_sp_best_loomis_split_rating, moody_sp_best_loomis_split_rating_band_name, moody_sp_best_loomis_split_rating_mapped, moody_sp_best_split_rating, moody_sp_best_split_rating_band_name, moody_sp_best_split_rating_mapped, moody_sp_fitch_average_split_rating, moody_sp_fitch_average_split_rating_band_name, moody_sp_fitch_average_split_rating_mapped, moody_sp_fitch_best_loomis_split_rating, moody_sp_fitch_best_loomis_split_rating_band_name, moody_sp_fitch_best_loomis_split_rating_mapped, moody_sp_fitch_best_split_rating, moody_sp_fitch_best_split_rating_band, moody_sp_fitch_best_split_rating_band_name, moody_sp_fitch_best_split_rating_band_name_mtg, moody_sp_fitch_best_split_rating_mapped, moody_sp_fitch_best_split_rating_mtg, moody_sp_fitch_duplicate_median_split_rating, moody_sp_fitch_duplicate_median_split_rating_band, moody_sp_fitch_duplicate_median_split_rating_band_name, moody_sp_fitch_duplicate_median_split_rating_mapped, moody_sp_fitch_median_loomis_split_rating, moody_sp_fitch_median_loomis_split_rating_band, moody_sp_fitch_median_loomis_split_rating_band_name, moody_sp_fitch_median_loomis_split_rating_mapped, moody_sp_fitch_median_split_rating, moody_sp_fitch_median_split_rating_band, moody_sp_fitch_median_split_rating_band_rating, moody_sp_fitch_median_split_rating_mapped, moody_sp_fitch_worst_loomis_split_rating, moody_sp_fitch_worst_loomis_split_rating_band_name, moody_sp_fitch_worst_loomis_split_rating_mapped, moody_sp_fitch_worst_split_rating, moody_sp_fitch_worst_split_rating_band_name, moody_sp_fitch_worst_split_rating_mapped, moody_sp_worst_loomis_split_rating, moody_sp_worst_loomis_split_rating_band_name, moody_sp_worst_loomis_split_rating_mapped, moody_sp_worst_split_rating, moody_sp_worst_split_rating_band_name, moody_sp_worst_split_rating_mapped, moody_split_rating, moody_split_rating_band_name, moody_split_rating_mapped, moody_underlying_rating, moody_watch, msaa_breakout, msaa_bucket_code, msaa_bucket_description, msaa_one, msaa_one_code, msaa_sector_ex_agency_cmbs, msaa_sector_tba_cash, msaa_securitized, msaa_securitized_ex_credit, msaa_securitized_tba_cash, msci_esg_adjusted_score, msci_esg_carbon_emiss_intensity_year, msci_esg_carbon_emiss_scope_1, msci_esg_carbon_emiss_scope_12, msci_esg_carbon_emiss_scope_12_inten, msci_esg_carbon_emiss_scope_12_key, msci_esg_carbon_emiss_scope_1_key, msci_esg_carbon_emiss_scope_2, msci_esg_carbon_emiss_scope_2_key, msci_esg_carbon_emiss_scope_3, msci_esg_carbon_emiss_scp_12_ind_inten, msci_esg_carbon_emiss_scp_12_inten_3yav, msci_esg_carbon_emiss_year, msci_esg_carbon_sales_intensity_recent, msci_esg_coal_potential_emissions, msci_esg_conv_oil_analyst_comments, msci_esg_corp_business_ethics_theme_score, msci_esg_corp_business_ethics_theme_weight, msci_esg_corp_carbon_emissions_score, msci_esg_corp_carbon_emissions_weight, msci_esg_corp_climate_change_theme_score, msci_esg_corp_climate_change_theme_weight, msci_esg_corp_corporate_gov_theme_score, msci_esg_corp_corporate_gov_theme_weight, msci_esg_corp_environ_opps_theme_score, msci_esg_corp_environ_opps_theme_weight, msci_esg_corp_environmental_pillar_score, msci_esg_corp_environmental_pillar_weight, msci_esg_corp_governance_pillar_score, msci_esg_corp_governance_pillar_weight, msci_esg_corp_govt_pub_pol_theme_score, msci_esg_corp_govt_pub_pol_theme_weight, msci_esg_corp_human_capital_theme_score, msci_esg_corp_human_capital_theme_weight, msci_esg_corp_natural_res_use_theme_score, msci_esg_corp_natural_res_use_theme_weight, msci_esg_corp_prod_carb_ftprnt_exp_score, msci_esg_corp_prod_carb_ftprnt_mgmt_score, msci_esg_corp_prod_carb_ftprnt_score, msci_esg_corp_prod_carb_ftprnt_weight, msci_esg_corp_product_safety_theme_score, msci_esg_corp_product_safety_theme_weight, msci_esg_corp_social_opps_theme_score, msci_esg_corp_social_opps_theme_weight, msci_esg_corp_social_pillar_score, msci_esg_corp_social_pillar_weight, msci_esg_corp_waste_mgmt_theme_score, msci_esg_corp_waste_mgmt_theme_weight, msci_esg_e_pillar_score, msci_esg_e_pillar_weight, msci_esg_g_pillar_score, msci_esg_g_pillar_weight, msci_esg_govn_env_pillar_weight, msci_esg_govn_gov_pillar_weight, msci_esg_govn_soc_pillar_weight, msci_esg_govt_env_pillar_score, msci_esg_govt_gov_pillar_score, msci_esg_govt_soc_pillar_score, msci_esg_is_industry_average, msci_esg_market_cap, msci_esg_met_coal_analyst_comments, msci_esg_met_coal_reserves_volume, msci_esg_natural_gas_analyst_comments, msci_esg_oil_nat_gas_potential_emiss, msci_esg_oil_potential_emissions, msci_esg_oil_sands_analyst_comments, msci_esg_oil_sands_potential_emissions, msci_esg_s_pillar_score, msci_esg_s_pillar_weight, msci_esg_shale_gas_analyst_comments, msci_esg_shale_oil_analyst_comments, msci_esg_shale_oil_gas_potential_emiss, msci_esg_th_coal_analyst_comments, msci_esg_total_potential_emissions, msci_esg_unconv_oil_gas_potential_emiss, msci_iva_numeric_rating, msci_iva_rating, msciesg_sin_stock_ae_max_rev_pct, msciesg_sin_stock_alc_max_rev, msciesg_sin_stock_gam_max_rev_pct, msciesg_sin_stock_tob_max_rev_pct, msciesg_sin_stock_weap_max_rev_pct, mtd_return, mtg_deal_name, mtg_generic_cpr_one_month, mtg_generic_cpr_six_month, mtg_generic_cpr_three_month, mtg_pool_cpr_one_month, mtg_pool_cpr_six_month, mtg_pool_cpr_three_month, mtg_tranche_typ, muni_issuer_des_2nd_line, muni_level_1, muni_level_2, muni_obligor, muni_prerefund_date, muni_purpose_class, muni_purpose_type, muni_quality_median, muni_sector, muni_state, muni_state_largest, muni_tax_prov, muni_tax_treatment, mv_multiplier, nace_sector_code, name, natural_gas_analyst_comments, next_call_date, next_call_price, ngic_level_four_name, obligor_bbid, ofac_foreign_financial_inst_sanction_status, ofac_full_sanc_company, ofac_non_sdn_iran_sanction_status, ofac_sectoral_sanc_co, ofac_sectoral_sanc_directive_four, ofac_sectoral_sanc_directive_one, ofac_sectoral_sanc_directive_three, ofac_sectoral_sanc_directive_two, oil_nat_gas_potential_emiss, oil_potential_emissions, oil_sands_analyst_comments, oil_sands_potential_emissions, omni_sector_level_1, omni_sector_level_2, option_adjusted_convexity, option_adjusted_duration, option_adjusted_spread, option_adjusted_spread_convexity, option_adjusted_spread_delta_180_day, option_adjusted_spread_delta_1day, option_adjusted_spread_delta_30day, option_adjusted_spread_delta_7day, option_adjusted_spread_delta_90day, option_adjusted_spread_delta_mtd, option_adjusted_spread_delta_qtd, option_adjusted_spread_delta_ytd, option_adjusted_spread_duration, option_adjusted_spread_duration_delta_180_day, option_adjusted_spread_duration_delta_1day, option_adjusted_spread_duration_delta_30day, option_adjusted_spread_duration_delta_7day, option_adjusted_spread_duration_delta_90day, option_adjusted_spread_duration_delta_mtd, option_adjusted_spread_duration_delta_qtd, option_adjusted_spread_duration_delta_ytd, option_adjusted_spread_percent_change_180_day, option_adjusted_spread_percent_change_1day, option_adjusted_spread_percent_change_30day, option_adjusted_spread_percent_change_7day, option_adjusted_spread_percent_change_90day, option_adjusted_spread_percent_change_mtd, option_adjusted_spread_percent_change_qtd, option_adjusted_spread_percent_change_ytd, order_reason, outlook, outstanding_face_value, p07d_return, p30d_return, p90d_return, parent_company_name, payment_rank, pigs, pls_empirical_duration, pls_empirical_duration_rsquared, pls_empirical_krd_10_year, pls_empirical_krd_2_year, pls_empirical_krd_30_year, pls_empirical_krd_5_year, pls_empirical_krd_6_month, prc_src_name, price_band, price_delta_180_day, price_delta_1day, price_delta_30day, price_delta_60day, price_delta_7day, price_delta_90day, price_delta_mtd, price_delta_qtd, price_delta_ytd, price_percent_change_180day, price_percent_change_1day, price_percent_change_30day, price_percent_change_60day, price_percent_change_7day, price_percent_change_90day, price_percent_change_mtd, price_percent_change_qtd, price_percent_change_ytd, private_placement, production_year, qtd_return, rating_band, rating_band_name, rating_id, rbc_health_current, rbc_life_current, rbc_life_naic_proposed, rbc_naic_bucket_current, rbc_naic_bucket_new, rbc_pc_current, re_remic, region_code, region_description, region_of_risk_code, region_of_risk_description, relative_return_id, relative_return_level_four_scheme, relative_return_level_one_scheme, relative_return_level_three_scheme, relative_return_level_two_scheme, rep_sec_id, reset_index_name, residual_spread, residual_spread_delta_30_day, residual_spread_latest, residual_spread_latest_date, residual_spread_previous_day, review_analyst, rich_cheap_10y, rich_cheap_10y_latest, rich_cheap_5y, rich_cheap_5y_latest, rich_cheap_cross_reference, rising_star_fallen_angel_flag, risk_industry_id, risk_industry_level_four_name, roll_residual_spread, roll_residual_spread_delta_30_day, roll_spread, roll_spread_delta_30_day, roll_spread_latest, roll_spread_latest_date, roll_spread_previous_day, rr_client_level_five_scheme, rr_client_level_four_scheme, rr_client_level_one_scheme, rr_client_level_six_scheme, rr_client_level_three_scheme, rr_client_level_two_scheme, rr_client_schema_1_tba_cash, rr_one_developed_em, rr_one_two, rr_three_cash_irf_treasury_combined, rr_two_developed_em, rr_yield_to_maturity, rule_144_a, rule_144_a_status, russell_level_four_code, russell_level_four_name, russell_level_one_code, russell_level_one_name, russell_level_three_code, russell_level_three_name, russell_level_two_code, russell_level_two_name, safe_industry, sales_annual, sales_annual_date, sales_quarterly, sales_quarterly_date, sanfran_only, sast_liquidity, sat_average_price, sat_beta, sat_distressed_liquidity, sat_liquidity, sat_vol_level, sat_zv_spread, sector_credit_outlook_rel, sector_level_1_name, sector_level_2_code, sector_level_2_name, securitized, securitized_custom_1, securitized_geo, securitized_is_jumbo, securitized_is_paid_off, securitized_lb_range, securitized_low_ln_cnt, securitized_ltv_range, securitized_maturity_band, securitized_orig_name, securitized_orig_tier, securitized_strategy, securitized_strategy_and_sub_strategy, securitized_strategy_tba_cash, securitized_sub_strategy, securitized_supervised, securitized_ts, securitized_wac_range, securitized_wal_buckets, securitized_wala_range, securitized_wp, security_bucket_all, security_id, security_target_id, sedol, senior, senior_most_tranche, seniority, sf_duration, sf_key_rate_duration_five_year, sf_key_rate_duration_seven_year, sf_key_rate_duration_six_months, sf_key_rate_duration_ten_year, sf_key_rate_duration_thirty_year, sf_key_rate_duration_twenty_year, sf_key_rate_duration_two_year, sf_option_adjusted_convexity, sf_option_adjusted_spread, sf_option_adjusted_spread_duration, sf_price, sf_risk_model_two_beta, sf_volatility_duration, shale_gas_analyst_comments, shale_oil_analyst_comments, shale_oil_gas_potential_emiss, shares_outstanding, short_name, singaporean_sanction_status, sink_type, sinkable, slope_10_30, slope_2_10, slope_2_5, slope_5_10, source_entity, source_id, sp_ccr_rating, sp_ccr_watch, sp_industry_level_four_name, sp_long_term_foreign_currency_debt_rating, sp_long_term_foreign_currency_issuer_rating, sp_long_term_local_currency_debt_rating, sp_long_term_local_currency_issuer_rating, sp_only_rating, sp_only_rating_band_name, sp_outlook, sp_recovery_rating, sp_split_rating, sp_split_rating_band_name, sp_split_rating_mapped, sp_underlying_rating, sp_watch, spec_pool_type, specific_volatility_daily, specific_volatility_monthly, sponsoring_entity_giin, spread_duration_plus_coupon_sensitivity, spread_scr, spread_times_spread_duration, spread_to_benchmark, st_outlook, standard_poors_band_rating, standard_poors_rating, state_of_incorporation, subordinate_type, sustain_esg_risk_category, sustain_esg_risk_percentile_subindustry, sustain_esg_risk_percentile_universe, sustain_esg_risk_rank_subindustry, sustain_esg_risk_rank_universe, sustain_esg_risk_score, sustain_esg_risk_score_avg_subindustry, sustain_esg_risk_score_avg_universe, sustain_esg_risk_score_date, sustain_overall_beta, sustain_overall_excess_exposure_score, sustain_overall_exposure_category, sustain_overall_exposure_score, sustain_overall_manageable_risk_factor, sustain_overall_manageable_risk_score, sustain_overall_managed_risk_score, sustain_overall_management_category, sustain_overall_management_gap_score, sustain_overall_management_score, sustain_overall_unmanageable_risk_score, sustainability_dimension, swiss_sanction_status, tba_payup, th_coal_analyst_comments, ticker, tik_pm_comments, tik_research_comments, topic_code, topic_name, total_potential_emissions, trader_id, trading_em_dev, traunch, try_return, ultimate, ultimate_company_code, ultimate_parent_company_name, ultimate_parent_corp_ticker, ultimate_parent_ticker_exchange, unconv_oil_gas_potential_emiss, underlying_id, united_nations_sanction_status, upside_12month_excess_return, upside_12month_return, urv_esg_score, urv_esg_score_change, urv_price, urv_price_src, vega_clean, volatility_duration, wac, wcaf_custom_1, yankee, yas_ispread_to_govt, years_to_average_life, years_to_effective_maturity, years_to_maturity, years_to_worst, yield, yield_source, yield_to_maturity, yield_to_worst, yield_to_worst_bands, ytd_return, zero_volatility_spread'
    valid_cols = valid_cols.split(', ')
    my_cols = cols.split(',')
    for col in my_cols:
        if col not in valid_cols:
            print('invalid field: ' + col)
            return False
    return True


def getSecServDataAsOf(sids='66857635,66831347,26041203,44479666,44939987,38486329,46860971,46776987,17', \
                       styp='rep_sec_id', asofdate='03/09/2021', \
                       cols='as_of_date,rep_sec_id,gps_sec_id,ticker,name,'+ \
                       'default_security_id,isin,cusip,cusip_9,sedol,bb_uniq_cd', \
                       wfile=False,vapi=2):
    """
    Inputs>>
    sids: comma delimitted list of security ids
    styp: a valid type (as per API documentation), default to rep_sec_id
    asofdate: comma delimitted list of dates in any of 7 formats:
        MM/dd/yyyy, MM-dd-yyyy, yyyy-MM-dd, yyyy/MM/dd, dd.MM.yyyy, dd-MMM-yyyy, MMM-dd-yyyy
        that is after 1999-09-29 and on or before today
    cols: comma delimitted list of valid fields (as per API documentation)
    wfile: boolean indicator whether to save data as csv file
    Outputs>>
    dataframe: data returned from api, None if invalid
    """
    # Validate Inputs
    if not isValidSecServFields(cols):
        return
    # Run Query on Security Service REST API
    if vapi == 1:
        url = 'http://in2apps/securityservice'
    elif vapi == 2:
        url = 'http://ace/prd/imt/secsvcv2'
    else:
        url = 'http://in2apps/securityservice'
    df = pd.read_json(url+'/query-securities-data?format=json'+ \
                      '&dates='+asofdate+ \
                      '&sectype='+styp+'&secids='+sids+ \
                      '&columns='+cols)
    # Export Data into CSV File
    if wfile:
        df.to_csv('data_'+str(datetime.datetime.now().timestamp())+'.csv')
    # Return DataFrame of Data
    return df


def getSecServDataFromTo(sids='66857635,66831347,26041203,44479666,44939987,38486329,46860971', \
                         styp='rep_sec_id', fromdate='04/19/2021', todate='04/23/2021', \
                         cols='as_of_date,rep_sec_id,dly_return,returnDays001', \
                         wfile=False,vapi=2):
    """
    Inputs>>
    sids: comma delimitted list of security ids
    styp: a valid type (as per API documentation), default to rep_sec_id
    fromdate, todate: dates in any of 7 formats:
        MM/dd/yyyy, MM-dd-yyyy, yyyy-MM-dd, yyyy/MM/dd, dd.MM.yyyy, dd-MMM-yyyy, MMM-dd-yyyy
        that is after 1999-09-29 and on or before today
    cols: comma delimitted list of valid fields (as per API documentation)
    wfile: boolean indicator whether to save data as csv file
    Outputs>>
    dataframe: data returned from api, None if invalid
    """
    # Validate Inputs
    if not isValidSecServFields(cols):
        return
    # Run Query on Security Service REST API
    if vapi == 1:
        url = 'http://in2apps/securityservice'
    elif vapi == 2:
        url = 'http://ace/prd/imt/secsvcv2'
    else:
        url = 'http://in2apps/securityservice'
    df = pd.read_json(url+'/query-securities-data?format=json'+ \
                      '&min-date='+fromdate+'&max-date='+todate+ \
                      '&sectype='+styp+'&secids='+sids+ \
                      '&columns='+cols)
    # Export Data into CSV File
    if wfile:
        df.to_csv('data_'+str(datetime.datetime.now().timestamp())+'.csv')
    # Return DataFrame of Data
    return df


__all__ = ["isValidSecServFields", "getSecServDataAsOf", "getSecServDataFromTo"]

