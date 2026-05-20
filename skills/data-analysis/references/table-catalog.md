# Data AI Business Table Catalog

Use this compact catalog to translate vague business requests into candidate
business data scopes. Treat it as a starting index; verify fields and join paths
through `data_exploration` before SQL analysis.

## Common Dimensions And Details

- Date dimension: `dim.dim_pub_date`.
- Staff department: `dim.dim_pub_staff_department`.
- Course info: `dim.dim_pub_course_info`.
- Public user detail: `dwd.dwd_d_cus_pub_user_detail_f`.
- WeCom follow, account, tag, and message scopes:
  `dwd.dwd_d_cus_pub_qwaccount_follow_info_f`,
  `dwd.dwd_d_cus_pub_qwaccount_follow_tag_f`,
  `dwd.dwd_d_cus_pub_qwaccount_user_f`,
  `dwd.dwd_d_use_bq_qwmsgsync_msg_f`.

## Reading Camp Users, Courses, And Study

- Reading camp course user facts: `dwd.dwd_d_cus_rc_course_user_f`.
- Reading camp course and lecture dimensions:
  `dim.dim_rc_course_info_f`, `dim.dim_rc_lecture_info`.
- Study summaries and records:
  `dws.dws_d_use_rc_user_study_lecture_sum_f`,
  `dwd.dwd_d_use_rc_user_study_record_f`,
  `dws.dws_d_use_rc_user_study_record_sum_f`,
  `ads.ads_d_use_rc_user_study_record_sum_f`.
- User close-course, lecture, level, medal, activity, challenge, relation, and
  certificate scopes use `dwd.dwd_d_use_rc_*` and `dwd.dwd_d_cus_rc_*` reading
  camp tables.

## Reading Camp Orders, GMV, Refunds, And Mall

- Order wide detail: `dws.dws_d_trd_rc_order_wide_detail_f`.
- Refund wide detail: `dws.dws_d_trd_rc_user_refund_wide_f`.
- Order and refund DWD details:
  `dwd.dwd_d_trd_rc_order_detail_f`,
  `dwd.dwd_d_trd_rc_order_refund_detail_f`,
  `dwd.dwd_d_trd_rc_outer_order_detail_f`.
- Leads-order attribution: `dwd.dwd_d_rc_trd_readcamp_leads_order_f`.
- Mall, commodity, inventory, gift, and warehouse scopes:
  `dwd.dwd_d_trd_rc_shopmall_order_f`,
  `dwd.dwd_d_cmd_rc_readcamp_shop_commodity_f`,
  `ads.ads_d_cmd_rc_shop_physical_item_f`.

## Reading Camp WeCom, Teacher, Sales Follow-Up

- User-teacher binding: `dwd.dwd_d_cus_rc_user_teacher_f`.
- Teacher info: `dim.dim_rc_teacher_info`.
- WeCom group, group user, group message, and leader scopes:
  `dwd.dwd_d_cus_rc_wechat_group_f`,
  `dwd.dwd_d_cus_rc_wechat_group_user_detail_f`,
  `dwd.dwd_d_cus_rc_wechat_group_msg_detail_f`,
  `dwd.dwd_d_cus_rc_wechat_group_leader_f`.
- Leader and teacher reply summaries:
  `dws.dws_d_use_rc_wechat_group_leader_reply_sum_day_f`,
  `ads.ads_m_rc_teacher_wechat_reply_sum_i`.

## Referral, Channels, And Ads

- Invitation and referral leads:
  `dwd.dwd_d_cus_rc_invitation_f`,
  `dwd.dwd_d_cus_rc_referral_leads_detail_f`.
- Allocation, source, and channel cost:
  `dws.dws_d_use_rc_user_allocate_detail_f`,
  `dws.dws_d_cus_rc_source_allocate_leads_detail_f`,
  `ods_manual.ods_manual_rc_dim_rc_source_cost_f`.
- Channel ROI and source transformation:
  `ads.ads_m_mrt_rc_leads_source_transform_gmv_cost_f`,
  `ads.ads_d_sale_rc_referral_gmv_output_ratio_month_f`.
- Rednote/XHS ads:
  `dwd.dwd_d_use_rc_xhs_creativity_ad_data_f`,
  `dwd.dwd_d_use_rc_xhs_user_ad_source_f`.

## Sales Performance

- Sales performance order and refund details:
  `dws.dws_d_sale_rc_performance_order_detail_f`,
  `dws.dws_d_sale_rc_performance_order_detail_f_v2`,
  `dws.dws_d_sale_rc_performance_order_refund_detail_f`.
- Performance collection, ratio, result, monthly order detail, target, and
  achievement:
  `ads.ads_d_rc_sale_performance_collect_data_f`,
  `ads.ads_d_rc_sale_performance_order_ratio_f`,
  `ads.ads_d_rc_sale_performance_data_f`,
  `ads.ads_d_sale_rc_sale_order_detail_month_f`,
  `ads.ads_rc_sale_gmv_target_data_v2_f`.

## Small-Class / RDC Classroom

- Classroom users, wide users, classes, rights, schedules, and study:
  `dwd.dwd_d_cus_rc_rdc_classroom_user_f`,
  `dws.dws_d_cus_rc_rdc_classroom_user_wide_f`,
  `dwd.dwd_d_cus_rc_rdc_classroom_user_class_f`,
  `dwd.dwd_d_cus_rc_user_class_hour_rights_f`,
  `dwd.dwd_d_cus_rc_rdc_classroom_user_schedule_f`.
- Classroom orders and refunds:
  `dws.dws_d_trd_rc_rdc_classroom_order_wide_f`,
  `dws.dws_d_trd_rc_rdc_classroom_order_refund_detail_f`.
- Paid users, wait-in-class users, advisor binding, tutor tags, teacher
  performance, and classroom status:
  `ads.ads_d_cus_rc_rdc_classroom_pay_user_detail_f`,
  `ads.ads_d_cus_rc_rdc_classroom_wait_inclass_user_detail_f`,
  `ads.ads_d_trd_rc_rdc_classroom_order_bind_rc_teacher_f`,
  `ads.ads_m_rc_class_teacher_performance_i`.

## MCN / Parent University

- User detail, user wide, rights, course, study, and activity:
  `dwd.dwd_d_cus_mcn_parentuniversity_user_detail_f`,
  `dws.dws_d_cus_mcn_parentuniversity_user_wide_f`,
  `dwd.dwd_d_cus_mcn_parentuniversity_user_rights_info_f`,
  `dwd.dwd_d_cus_mcn_parentuniversity_user_courses_f`.
- Orders, refunds, third-party orders, inner orders, and finance accounting:
  `dws.dws_d_trd_mcn_parentuniversity_order_wide_f`,
  `dws.dws_d_trd_mcn_parentuniversity_order_refund_detail_f`,
  `dwd.dwd_d_trd_mcn_parentuniversity_third_party_order_f`,
  `ads.ads_d_trd_mcn_parentuniversity_sp_order_accounting_f`.

## Public External Orders

- Tencent, Douyin, and XHS external order/refund ODS tables feed public DWD
  external order product and refund tables:
  `dwd.dwd_d_trd_pub_order_pay_outer_order_product_info_f`,
  `dwd.dwd_d_trd_pub_order_pay_outer_order_refund_detail_f`.
