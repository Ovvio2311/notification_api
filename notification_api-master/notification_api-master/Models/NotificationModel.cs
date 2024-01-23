using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace notification_api.Models
{
    public enum NotifType
    {
        auto = 0,
        email = 1,
        sms = 2,
        apps = 3,
        system = 4
    }
    public class BaseResponse
    {
        public int result { get; set; }
        public string result_time { get; set; } = DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss");
        public string code { get; set; }
        public string sys_message { get; set; }
        public string display_message { get; set; }
    }
    public class ProducerResponse
    {
        public string ProducerMsg { get; set; }
        public bool IsApiSuccess { get; set; }
    }

    public class NotiApiResponse
    {        
        public bool Success { get; set; }
    }

    public class NotiApiResult
    {
        public bool Success { get; set; }
        public int result { get; set; }
        public string result_time { get; set; } = DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss");
        public string code { get; set; }
        public string sys_message { get; set; }
        public string display_message { get; set; }
    }

    public enum Priority
    {
        low = 3,
        medium = 2,
        high = 1
    }

    public class sendNotificationObj
    {
        public string message_type { get; set; }
        public string notification_type { get; set; }
        public Priority priority { get; set; }
        public string? schedule_datetime { get; set; }
        public string[] content_values { get; set; }
        public long? target_user_id { get; set; }
        public bool is_perm_acct { get; set; }
        public string email_address { get; set; }
        public string mobile_phone { get; set; }
        public long? bi_veh_id { get; set; }
        public string notification_lang { get; set; } = "tc";
    }

    public class acct_noti_preference
    {
        public bool? b_sms { get; set; }
        public bool? b_email { get; set; }
        public bool? b_apps { get; set; }
        public bool? acct_sms { get; set; }
        public bool? acct_email { get; set; }
        public bool? acct_apps { get; set; }
        public short? method_priority { get; set; }
        public bool? b_display_on_ui { get; set; }
        public bool? b_custom_configurable { get; set; }
        public byte? ti_message_category_id { get; set; }
        public string vc_sys_noti_desc_en { get; set; }
    }

    public class TemplateDetail
    {
        public string template_title { get; set; }
        public string template_content { get; set; }
    }

    public class NotificationSendRequest
    {
        public int notification_message_id { get; set; }
        public long account_id { get; set; }
        public bool is_perm_acct { get; set; }
        public string message_type { get; set; }
        public string notification_type { get; set; }
        public string notification_lang { get; set; }
        public string[] content_values { get; set; }
        public string email_address { get; set; }
        public string mobile_phone { get; set; }
    }

    public class AddAcctNotificationRequest
    {
        public long? account_id { get; set; }
        public bool? is_perm_acct { get; set; }
        public string message_title { get; set; }
        public string message_content { get; set; }
        public byte? message_category_id { get; set; }
    }
    public class broadcast_acct_info
    {
        public long? bi_acct_id { get; set; }
        public string? vc_acct_id { get; set; }
        public string? tk_mobile_phone { get; set; }
        public string? tk_acct_email { get; set; }
        public int? ti_notification_lang { get; set; }
    }

    public class tb_message_queue_with_priority
    {
        public long? target_user_id { get; set; }
        public string message_type { get; set; }
        public string type { get; set; }
        public string notification_lang { get; set; }
        public string email_address { get; set; }
        public string mobile_phone { get; set; }
        public string content_values_str { get; set; }
        public DateTime? schedule_datetime { get; set; }
        public bool? is_broadcast { get; set; }
        public bool? is_push_app { get; set; }
        public string push_app_acct_id { get; set; }
        public string recipient { get; set; }
        public string notification_message_id { get; set; }
        public bool? is_perm_acct { get; set; }
        public byte? message_category_id { get; set; }
        public string vc_noti_type { get; set; }
        public string vc_noti_lang { get; set; }
        public string vc_noti_preference { get; set; }
    }
}
