using Dapper;

using FFTS.InternalCommonDBUtils;
using FFTS.InternalCommonDBUtils.Models;
using Microsoft.Extensions.Configuration;

using Newtonsoft.Json;

using System;
using System.Globalization;
using System.Threading.Tasks;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FFTS.InternalCommonModels.APIRequestObjects;
using System.Diagnostics;
using FFTS.InternalCommonUtils;
using Microsoft.Extensions.Logging;
using notification_api.Models;
using FFTS.IOptions;

namespace notification_api.Services
{
    public class NotificationService
    {
        private readonly IConfiguration _configuration;

        private Db _db;
        private string connstring;
        private DataEncryption _encryption;

        private IDbConnection _conn = null;
        private readonly bool initDBConn = false;

        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger<NotificationService> _logger;
        public NotificationService(IConfiguration configuration, ILoggerFactory loggerFactory, ILogger<NotificationService> logger = null, IDbConnection conn = null)
        {
            _configuration = configuration;
            _db = new Db(_configuration);

            _loggerFactory = loggerFactory;
            _logger = logger;
            
            _encryption = new DataEncryption();

            if (conn == null || conn.State != ConnectionState.Open)
            {
                _conn = _db.getIDbOpenConnection();
                initDBConn = true;
            }
            else
            {
                _conn = conn;
            }
        }

        public async Task<acct_noti_preference> get_acct_noti_preference(string message_type, long acct_id, bool is_perm_acct)
        {
            _logger.LogDebug($"get_acct_noti_preference - conn state -> {_conn.State.ToString()}");

            acct_noti_preference return_acct_noti_preference = new acct_noti_preference();
            try
            {
                string SQLString = "";
                if (acct_id > 0)
                {
                    if (is_perm_acct)
                    {
                        SQLString = @"SELECT CASE WHEN a.b_sms = 1 AND mt.ti_Sms = 1 THEN 1 ELSE 0 END AS b_sms,  
                                CASE WHEN a.b_email = 1 AND mt.ti_Email = 1 THEN 1 ELSE 0 END AS b_email,  
                                CASE WHEN a.b_apps = 1 AND mt.ti_Apps = 1 THEN 1 ELSE 0 END AS b_apps,  
                                c.b_sms AS acct_sms, c.b_email AS acct_email, c.b_apps AS acct_apps,  
                                g.dc_config_value AS method_priority, a.b_display_on_ui, a.b_custom_configurable, mt.ti_message_category_id, a.vc_sys_noti_desc_en  
                                FROM tb_sys_noti_preference a
                                JOIN tb_sys_noti_msg_mapping b ON a.bi_sys_noti_preference_id = b.bi_sys_noti_preference_id  
                                JOIN tb_acct_noti_preference c ON c.bi_sys_noti_preference_id = a.bi_sys_noti_preference_id 
                                JOIN tb_acct acct ON c.bi_acct_id = acct.bi_acct_id
                                JOIN tb_sys_noti_acct_type_mapping d ON d.bi_sys_noti_preference_id = a.bi_sys_noti_preference_id AND d.ti_acct_type_id = acct.ti_acct_type_id
                                LEFT JOIN tb_message_type mt ON mt.vc_Message_Type = b.vc_message_type  
                                LEFT JOIN tb_config_general g ON g.vc_config_code = 'NOTIFICATION_METHOD_PRIORITY'  
                                WHERE b.vc_message_type = @message_type AND a.b_enable = 1 AND mt.ti_Enabled = 1 AND acct.bi_acct_id = @acct_id; ";
                    }
                    else
                    {
                        SQLString = @"SELECT CASE WHEN a.b_sms = 1 AND mt.ti_Sms = 1 THEN 1 ELSE 0 END AS b_sms,  
                                CASE WHEN a.b_email = 1 AND mt.ti_Email = 1 THEN 1 ELSE 0 END AS b_email,  
                                CASE WHEN a.b_apps = 1 AND mt.ti_Apps = 1 THEN 1 ELSE 0 END AS b_apps,  
                                c.b_sms AS acct_sms, c.b_email AS acct_email, c.b_apps AS acct_apps,  
                                g.dc_config_value AS method_priority, a.b_display_on_ui, a.b_custom_configurable, mt.ti_message_category_id, a.vc_sys_noti_desc_en  
                                FROM tb_sys_noti_preference a
                                JOIN tb_sys_noti_msg_mapping b ON a.bi_sys_noti_preference_id = b.bi_sys_noti_preference_id  
                                JOIN tb_registration_acct_noti_preference c ON c.bi_sys_noti_preference_id = a.bi_sys_noti_preference_id 
                                JOIN tb_registration_acct acct ON c.bi_acct_reg_id = acct.bi_acct_reg_id
                                JOIN tb_sys_noti_acct_type_mapping d ON d.bi_sys_noti_preference_id = a.bi_sys_noti_preference_id AND d.ti_acct_type_id = acct.ti_acct_type_id
                                LEFT JOIN tb_message_type mt ON mt.vc_Message_Type = b.vc_message_type  
                                LEFT JOIN tb_config_general g ON g.vc_config_code = 'NOTIFICATION_METHOD_PRIORITY'  
                                WHERE b.vc_message_type = @message_type AND a.b_enable = 1 AND mt.ti_Enabled = 1 AND acct.bi_acct_reg_id = @acct_id; ";
                    }
                }
                else
                {
                    SQLString = @"SELECT CASE WHEN a.b_sms = 1 AND mt.ti_Sms = 1 THEN 1 ELSE 0 END AS b_sms,  
                                CASE WHEN a.b_email = 1 AND mt.ti_Email = 1 THEN 1 ELSE 0 END AS b_email,  
                                CASE WHEN a.b_apps = 1 AND mt.ti_Apps = 1 THEN 1 ELSE 0 END AS b_apps,  
                                0 AS acct_sms, 0 AS acct_email, 0 AS acct_apps,  
                                g.dc_config_value AS method_priority, a.b_display_on_ui, a.b_custom_configurable, mt.ti_message_category_id, a.vc_sys_noti_desc_en  
                                FROM tb_sys_noti_preference a
                                JOIN tb_sys_noti_msg_mapping b ON a.bi_sys_noti_preference_id = b.bi_sys_noti_preference_id  
                                LEFT JOIN tb_message_type mt ON mt.vc_Message_Type = b.vc_message_type  
                                LEFT JOIN tb_config_general g ON g.vc_config_code = 'NOTIFICATION_METHOD_PRIORITY'  
                                WHERE b.vc_message_type = @message_type AND a.b_enable = 1 AND mt.ti_Enabled = 1; ";
                }

                var para = new DynamicParameters();
                para.Add("message_type", message_type);
                para.Add("acct_id", acct_id);
                var result = await _db.QueryAsync<acct_noti_preference>(SQLString, para, _conn);
                return_acct_noti_preference = result.FirstOrDefault();

            }
            catch (Exception ex) {
                _logger.LogDebug($"get_acct_noti_preference - exception -> {ex.ToString()}");
            } 
            finally
            {
                //if (initDBConn) _db.CloseIDbConnection(ref _conn);
            }

            return return_acct_noti_preference;
        }

        public async Task<byte?> get_message_cat_id(string message_type)
        {
            try
            {
                string SQLString = "select ifnull(ti_message_category_id,0) from tb_message_type " +
                                    "where vc_Message_Type = @message_type;";

                var para = new DynamicParameters();
                para.Add("@message_type", message_type);
                var result = await _db.QueryAsync<byte>(SQLString, para, _conn);

                return result.FirstOrDefault();
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task<string> get_sys_noti_desc(string message_type)
        {
            try
            {
                string SQLString = "select a.vc_sys_noti_desc_en from " +
                                    "tb_sys_noti_preference a, tb_sys_noti_msg_mapping b " +
                                    "where a.bi_sys_noti_preference_id = b.bi_sys_noti_preference_id " +
                                    "and b.vc_message_type = @message_type AND a.b_enable = 1; ";

                var para = new DynamicParameters();
                para.Add("@message_type", message_type);
                var result = await _db.QueryAsync<string>(SQLString, para, _conn);

                return result.FirstOrDefault();
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task<TemplateDetail> get_template_content(NotificationSendRequest data, string language)
        {
            TemplateDetail templateDetail = new TemplateDetail();

            try
            {
                string SQLString = "SELECT vc_Title as template_title, convert(b_Template_Content, char) as template_content FROM tb_notification_template_content WHERE bi_Template_ID = " +
                    "(select bi_Template_ID from tb_notification_template where dt_Effective_DateTime<current_timestamp and i_Template_status = 3 AND vc_Message_type = @message_type " +
                    "ORDER BY dt_Effective_DateTime DESC LIMIT 1) and vc_Language = @language and vc_Type = @type;";

                var para = new DynamicParameters();
                para.Add("message_type", data.message_type);
                para.Add("language", language);
                para.Add("type", data.notification_type);
                var result = await _db.QueryAsync<TemplateDetail>(SQLString, para, _conn);
                templateDetail = result.FirstOrDefault();
            }
            catch (Exception ex) { }
            finally
            {
                //if (initDBConn) _db.CloseIDbConnection(ref _conn);
            }

            return templateDetail;
        }
        public async Task<bool> isBroadcast(string message_type)
        {
            bool result = false;

            try
            {
                string SQLString = "SELECT ti_Broadcast FROM tb_message_type where vc_Message_Type = @message_type";

                var para = new DynamicParameters();
                para.Add("message_type", message_type);
                var ti_Broadcast = await _db.ExecuteScalarAsync(SQLString, para, _conn);
                result = bool.Parse(ti_Broadcast.ToString());
            }
            catch (Exception ex) { }

            return result;
        }

        public async Task<List<broadcast_acct_info>> get_broadcast_acct_info(string type)
        {
            List<broadcast_acct_info> list = new List<broadcast_acct_info>();

            try
            {
                string SQLString = "";

                switch (type)
                {
                    case "apps":
                        //SQLString = "SELECT DISTINCT a_1.bi_acct_id, a_1.vc_acct_id, a_1.ti_notification_lang FROM tb_acct a_1 LEFT JOIN tb_acct_phone p_1 ON a_1.bi_acct_id = p_1.bi_acct_id " + 
                        //    "WHERE a_1.bi_acct_id IN (SELECT MAX(a.bi_acct_id) FROM tb_acct a LEFT JOIN tb_acct_phone p ON a.bi_acct_id = p.bi_acct_id " + 
                        //    "WHERE a.ti_acct_status_id = 1 AND a.ti_acct_type_id != 8 AND p.tk_mobile_phone IS NOT NULL GROUP BY tk_mobile_phone);";
                        SQLString = "SELECT DISTINCT a_1.bi_acct_id, a_1.vc_acct_id, a_1.ti_notification_lang " +
                            "FROM tb_acct a_1 " +
                            "WHERE a_1.ti_acct_status_id = 1 AND a_1.ti_acct_type_id != 8 ";
                        break;
                    case "sms":
                        SQLString = "SELECT DISTINCT p_1.tk_mobile_phone, a_1.bi_acct_id, a_1.ti_notification_lang FROM tb_acct a_1 LEFT JOIN tb_acct_phone p_1 ON a_1.bi_acct_id = p_1.bi_acct_id " +
                            "WHERE a_1.bi_acct_id IN (SELECT MAX(a.bi_acct_id) FROM tb_acct a LEFT JOIN tb_acct_phone p ON a.bi_acct_id = p.bi_acct_id " +
                            "WHERE a.ti_acct_status_id = 1 AND a.ti_acct_type_id != 8 AND p.tk_mobile_phone IS NOT NULL GROUP BY tk_mobile_phone);";
                        break;
                    case "email":
                        SQLString = "SELECT DISTINCT e_1.tk_acct_email, a_1.bi_acct_id, a_1.ti_notification_lang FROM tb_acct a_1 LEFT JOIN tb_acct_email e_1 ON a_1.bi_acct_id = e_1.bi_acct_id " +
                            "WHERE a_1.bi_acct_id IN (SELECT MAX(a.bi_acct_id) FROM tb_acct a LEFT JOIN tb_acct_email e ON a.bi_acct_id = e.bi_acct_id " +
                            "WHERE a.ti_acct_status_id = 1 AND a.ti_acct_type_id != 8 AND e.tk_acct_email IS NOT NULL GROUP BY tk_acct_email);";
                        break;
                }

                var result = await _db.QueryAsync<broadcast_acct_info>(SQLString, null, _conn);
                list = result.ToList();
            }
            catch (Exception ex) { }

            return list;
        }

        public async Task<bool> bes_cre_db020_add_acct_notification(AddAcctNotificationRequest req)
        {
            bool isSuccess;

            try
            {
                if (req.account_id != null)
                {
                    string sql = "INSERT INTO tb_acct_notification " +
                        "(bi_acct_id, vc_title, vc_message, ti_acct_notification_status, b_read, ti_message_category_id, vc_create_user_id " +
                        ", dt_create_datetime, vc_update_user_id, si_update_user_id, dt_last_update_datetime) " +
                        "VALUES(@account_id, @message_title, @message_content, 1, 0, @message_category_id, '', NOW(), '', 0, NOW())";

                    if (req.is_perm_acct == false)
                    {
                        sql = "INSERT INTO tb_registration_acct_notification " +
                        "(bi_reg_acct_id, vc_title, vc_message, ti_acct_notification_status, b_read, ti_message_category_id, vc_create_user_id " +
                        ", dt_create_datetime, vc_update_user_id, si_update_user_id, dt_last_update_datetime) " +
                        "VALUES(@account_id, @message_title, @message_content, 1, 0, @message_category_id, '', NOW(), '', 0, NOW())";
                    }

                    var para = new DynamicParameters();
                    para.AddDynamicParams(req);

                    await _db.ExecuteAsync(sql, para, false, _conn);
                }

                isSuccess = true;
            }
            catch (Exception ex)
            {
                isSuccess = false;
            }

            return isSuccess;
        }

        public async Task<bool> bes_cre_db019_notification_send(string notification_message_id)
        {
            bool isSuccess;

            try
            {
                string sql = "Update tb_message_queue set ti_Status = @status where bi_ID = @notification_message_id";

                var para = new DynamicParameters();
                
                para.Add("notification_message_id", notification_message_id);
                para.Add("status", 1);
                await _db.ExecuteAsync(sql, para, false, _conn);

                string sql2 = "Insert into tb_message_queue_child (bi_ID, ti_Status) values (@notification_message_id, @status)";

                var para2 = new DynamicParameters();
                para2.Add("notification_message_id", notification_message_id);
                para2.Add("status", 1);
                await _db.ExecuteAsync(sql2, para2, false,
                    _conn);

                isSuccess = true;
            }
            catch (Exception ex)
            {
                isSuccess = false;
            }

            return isSuccess;
        }
    }

    
}
