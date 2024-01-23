using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using notification_api.Models;
using Pulsar.Client.Api;
using FFTS.InternalCommonUtils;
using FFTS.InternalCommonDBUtils;
using Dapper;
using System.Globalization;
using System.Runtime;
using System.Data;
using FFTS.InternalCommonModels.DBModels.ACCMGT.Account;
using notification_api.Services;
using FFTS.InternalCommonModels.APIRequestObjects.TokenizationAPI;
using System.Linq;
using FFTS.IOptions;
using System.Runtime.CompilerServices;

namespace notification_api
{
    public class NotificationUtils
    {
        private string _pulsarServiceUrl { get; set; }
        private string _pulsarCert { get; set; }
        private string _topicHighPriority { get; set; }
        private string _topicMediumPriority { get; set; }
        private string _topicLowPriority { get; set; }

        //private static PulsarClient client;
        private static ConcurrentDictionary<string, IProducer<string>> _producerDict = new ConcurrentDictionary<string, IProducer<string>>();

        public static ProducerResponse ProducerReturn { get; set; }

        private readonly IConfiguration _config;
        private readonly ILogger<NotificationUtils> _logger;
        private readonly ILoggerFactory _loggerFactory;

        private IMemoryCache _cache;

        private Db _db;
        private DataEncryption _encryption;
        private NotificationService _notificationService;
        private TokenizationService _tokenService;
        private AccountSearchService _acctSearchService;
        private PaymentAndBillingService _paymentAndBillingService;
        private readonly PulsarProducerService _pulsarProducerService;
        private readonly PushService _pushService;

        private readonly SendNotification _SendNotificationConfig;

        public IDbConnection connection = null;

        public static readonly Stopwatch _stopwatch = new Stopwatch();

        public class NotiMsgReceiveRequest
        {
            public long user_id { get; set; }
            public string message_type { get; set; }
            public string notification_type { get; set; }
            public string[] content_values { get; set; }
            public long? target_user_id { get; set; }
            public bool? is_perm_acct { get; set; }
            public string? schedule_datetime { get; set; }
            public string email_address { get; set; }
            public string mobile_phone { get; set; }
            public string notification_lang { get; set; }
        }

        public NotificationUtils(IConfiguration configuration, PulsarProducerService pulsarProducerService, ILoggerFactory loggerFactory, ILogger<NotificationUtils> logger, IMemoryCache cache, PulsarClient Client)
        {
            _config = configuration;

            _pulsarServiceUrl = _config["SendNotification:PulsarUri"];
            //_pulsarCert = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), _config["SendNotification:tlsCertPath"], _config["SendNotification:tlsCertFile"]);
            _topicHighPriority = _config["SendNotification:Topics:HighPriority"];
            _topicMediumPriority = _config["SendNotification:Topics:MediumPriority"];
            _topicLowPriority = _config["SendNotification:Topics:LowPriority"];

            _cache = cache;

            //client = Client;

            _loggerFactory = loggerFactory;
            _logger = logger;

            _SendNotificationConfig = _config.GetSection("SendNotification").Get<SendNotification>();

            _db = new Db(_config);
            _encryption = new DataEncryption();

            if (_cache != null)
            {
                if (!_cache.TryGetValue("ProducerDict", out _producerDict)) _producerDict = new ConcurrentDictionary<string, IProducer<string>>();
            }

            bool enablePulsarClientLogger;

            if (bool.TryParse(_config["SendNotification:EnablePulsarClientLogger"], out enablePulsarClientLogger) && enablePulsarClientLogger)
                PulsarClient.Logger = _logger;


            _notificationService = new NotificationService(_config, _loggerFactory, _loggerFactory.CreateLogger<NotificationService>(), connection);
            _tokenService = new TokenizationService(_loggerFactory.CreateLogger<TokenizationService>(), _config);
            _acctSearchService = new AccountSearchService(_config);
            _pushService = new PushService(_config, _loggerFactory, _loggerFactory.CreateLogger<PushService>(), connection);
            _paymentAndBillingService = new PaymentAndBillingService(_config);
            _pulsarProducerService = pulsarProducerService;
        }

        public async Task<NotiApiResult> SendNotificationV2Async(string message_type, string notification_type, Priority priority, string schedule_datetime, string[] content_values,
            long? target_user_id, bool? is_perm_acct, string email_address, string mobile_phone, string notification_lang = "tc")
        {
            NotiApiResult result = new();

            result.Success = false;

            DateTime dt = DateTime.Now;
            _logger.LogInformation($"target_user_id -> {target_user_id} - [SNV001]SendNotificationV2Async: incoming request, message_type: -> {message_type}, notification_type -> {notification_type}, " +
                $"schedule_datetime -> {schedule_datetime}, email_address -> {DataMasking.emailMask(email_address)}, mobile_phone -> {DataMasking.phoneNumberMask(mobile_phone)}");

            if (connection != null && connection.State !=  ConnectionState.Open) connection = _db.getIDbOpenConnection();

            try
            {
                DynamicParameters pPara = new();
                pPara.Add("vc_message_type", message_type);

                string pSQL = "select ti_priority from tb_message_type where vc_message_type = @vc_message_type;";
                byte? msgPriority = (await _db.QueryAsync<byte?>(pSQL, pPara, connection)).FirstOrDefault();

                if (msgPriority != null)
                {
                    if (msgPriority != 0)
                    {
                        priority = (Priority)msgPriority;
                    }                    
                }

                long? _account_id = target_user_id == null ? 0 : target_user_id;
                bool? _is_perm_acct = is_perm_acct ?? false;

                DateTime _scheduleDT = new();
                DateTime.TryParseExact(schedule_datetime, _config["DateFormat:DTTIME"], null, System.Globalization.DateTimeStyles.None, out _scheduleDT);

                string _notifType = string.IsNullOrEmpty(notification_type) ? "auto" : notification_type;

                string _email_address = string.IsNullOrEmpty(email_address) ? null : email_address;
                string _mobile_phone = string.IsNullOrEmpty(mobile_phone) ? null : mobile_phone;

                string _notification_lang = notification_lang ?? "tc";

                long _user_id = 1;
                string _message_type = message_type;
                string[] _content_values = content_values;

                string tableName = string.Empty;
                switch (priority)
                {
                    case Priority.high:
                        tableName = "tb_message_queue_high";
                        break;
                    case Priority.medium:
                        tableName = "tb_message_queue_medium";
                        break;
                    case Priority.low:
                        tableName = "tb_message_queue_low";
                        break;
                    default:
                        break;
                }

                string _notification_message_id = await getNextDocNoAsync("NOTI_MSG");

                if (string.IsNullOrWhiteSpace(_notification_lang))
                    _notification_lang = "tc";

                if (_account_id == -1 && await _notificationService.isBroadcast(_message_type))
                {
                    _logger.LogDebug($"notif_msg_id -> {_notification_message_id} - [NP006A]Send Broadcast Start, notifType -> {_notifType} ");

                    var data = new
                    {
                        recipient = "All",
                        message_title = "",
                        message_content = "",
                        notification_message_id = _notification_message_id,
                        message_type = _message_type
                    };

                    List<broadcast_acct_info> acctList = await _notificationService.get_broadcast_acct_info(_notifType);
                    byte? msgCatID = await _notificationService.get_message_cat_id(_message_type);
                    string sysNotiDesc = await _notificationService.get_sys_noti_desc(_message_type);

                    acct_noti_preference bc_noti_Preference = new() { ti_message_category_id = msgCatID, vc_sys_noti_desc_en = sysNotiDesc };

                    string sql = $"Insert into {tableName} (" +
                                "bi_user_id, vc_message_type, vc_type, vc_language, vc_email_address, vc_mobile_phone, vc_content_values, i_fail_count, dt_schedule_datetime, ti_status, " +
                                $"b_broadcast, vc_recipient, vc_notification_message_id, b_push_app, vc_push_app_acct_id, b_is_perm_acct, ti_message_category_id, bi_create_user_id, dt_create_dateTime " +
                                $") values ";

                    string insertSQL = sql;
                    int count = 0;

                    bool hasMsg = false;
                    List<string> insertPara = new();
                    foreach (broadcast_acct_info acct in acctList)
                    {
                        count++;
                        var para = await GetMessageQueueData(acct.bi_acct_id, _is_perm_acct, _notifType, acct.tk_acct_email, acct.tk_mobile_phone, acct.ti_notification_lang.ToString(), bc_noti_Preference, true);
                        para.message_type = _message_type;
                        para.content_values_str = JsonConvert.SerializeObject(_content_values);
                        para.schedule_datetime = _scheduleDT == DateTime.MinValue ? null : _scheduleDT;
                        para.notification_message_id = _notification_message_id;

                        para.mobile_phone = string.IsNullOrEmpty(para.mobile_phone) ? null : _encryption.fnDBSetAESString(para.mobile_phone);
                        para.email_address = string.IsNullOrEmpty(para.email_address) ? null : _encryption.fnDBSetAESString(para.email_address);
                        para.recipient = string.IsNullOrEmpty(para.recipient) ? null : _encryption.fnDBSetAESString(para.recipient);

                        if (!string.IsNullOrWhiteSpace(para.type) && para.type != "ignored" && para.type != "apps") {

                            insertPara.Add($"({para.target_user_id},'{para.message_type}','{para.type}','{para.notification_lang}',{(para.email_address != null ? "'" + para.email_address + "'" : "NULL")}, {(para.mobile_phone != null ? "'" + para.mobile_phone + "'" : "NULL")},{(para.content_values_str != null ? "'" + para.content_values_str + "'" : "NULL")}," +
                                $"0,{(para.schedule_datetime != null ? "'" + para.schedule_datetime + "'" : "NULL")},0,{(para.is_broadcast != null ? ((bool)para.is_broadcast ? 1 : 0) : 0)},{(para.recipient != null ? "'" + para.recipient + "'" : "NULL")},'{para.notification_message_id}',{(para.is_push_app != null ? ((bool)para.is_push_app ? 1 : 0) : "NULL")},{(para.push_app_acct_id != null ? "'" + para.push_app_acct_id + "'" : "NULL")},{((para.is_perm_acct ?? false) ? 1 : 0)}," +
                                $"{(para.message_category_id != null ? "'" + para.message_category_id + "'" : "NULL")},'{_user_id}',sysdate() " +
                                ")");
                            hasMsg = true;
                        }
                        
                        if (para.is_push_app ?? false) {
                            count++;
                            insertPara.Add($"({para.target_user_id},'{para.message_type}','apps','{para.notification_lang}',{(para.email_address != null ? "'" + para.email_address + "'" : "NULL")}, {(para.mobile_phone != null ? "'" + para.mobile_phone + "'" : "NULL")},{(para.content_values_str != null ? "'" + para.content_values_str + "'" : "NULL")}," +
                                $"0,{(para.schedule_datetime != null ? "'" + para.schedule_datetime + "'" : "NULL")},0,{(para.is_broadcast != null ? ((bool)para.is_broadcast ? 1:0) : 0)},{(para.recipient != null ? "'" + para.recipient + "'" : "NULL")},'{para.notification_message_id}',{(para.is_push_app != null ? ((bool)para.is_push_app ? 1 : 0) : "NULL")},{(para.push_app_acct_id != null ? "'" + para.push_app_acct_id + "'" : "NULL")},{((para.is_perm_acct ?? false)? 1: 0)}," +
                                $"{(para.message_category_id != null ? "'" + para.message_category_id + "'" : "NULL")},'{_user_id}',sysdate() " +
                                ")");
                            hasMsg = true;
                        }

                        if (count >= 1000 || acctList.IndexOf(acct) == acctList.Count - 1)
                        {
                            count = 0;
                            string iSQL = insertSQL + string.Join(", " ,insertPara);

                            //insertSQL = sql;
                            if (hasMsg) await _db.ExecuteAsync(iSQL, null, false, connection);

                            insertPara = new();
                        }
                    }
                    result.Success = true;
                    _stopwatch.Restart();
                }
                else
                {
                    #region Cater VehiclePassageNoticeRVO
                    if (message_type.ToLower()== "vehiclepassagenoticervo")
                    {
                        if (_content_values != null)
                        {
                            if (!string.IsNullOrEmpty(_content_values[0]))
                            {
                                long cust_trxn_id = 0;

                                if (long.TryParse(_content_values[0], out cust_trxn_id))
                                {
                                    if (cust_trxn_id > 0)
                                    {
                                        var GetShortLinkValResult = await _paymentAndBillingService.GetRVOSevElevShortLink(cust_trxn_id);
                                        if (GetShortLinkValResult.result)
                                        {
                                            var shortLinkVal = GetShortLinkValResult.short_link_val;
                                            Array.Resize(ref _content_values, _content_values.Length + 1);
                                            _content_values[_content_values.Length - 1] = shortLinkVal;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    #endregion

                    if (message_type.ToLower() == "vehiclepassagenotice" || message_type.ToLower() == "vehiclepassagenoticervo" || message_type.ToLower() == "vehiclepassagenoticett")
                    {
                        if (_content_values != null)
                        {
                            if (!string.IsNullOrEmpty(_content_values[1]) && !string.IsNullOrEmpty(_content_values[4]))
                            {
                                if (_content_values[1].Contains("Cross Harbour Tunnel"))
                                {
                                    DateTime repostDate = new DateTime(2023, 10, 20);
                                    DateTime passageDate = DateTime.ParseExact(_content_values[4], "dd/MM/yyyy, HH:mm",
                                       System.Globalization.CultureInfo.InvariantCulture);

                                    if (passageDate < repostDate)
                                    {
                                        if (message_type.ToLower() == "vehiclepassagenoticett")
                                        {
                                            _message_type = "RepostVehiclePassageNoticeTT";
                                            message_type = "RepostVehiclePassageNoticeTT";
                                        }
                                        else if (message_type.ToLower() == "vehiclepassagenoticervo")
                                        {
                                            _message_type = "RepostVehiclePassageNoticeRVO";
                                            message_type = "RepostVehiclePassageNoticeRVO";
                                        }
                                        else if (message_type.ToLower() == "vehiclepassagenotice")
                                        {
                                            //check the acct has autopayment or not.
                                            var ViewPaymentTokenResult = await _paymentAndBillingService.ViewPaymentTokenList(_account_id.GetValueOrDefault());
                                            if (ViewPaymentTokenResult.auto_payment_token_list.Any(a=>a.ti_auto_payment_token_status_id == 1))
                                            {
                                                _message_type = "RepostVehiclePassageNoticeWithAutopay";
                                                message_type = "RepostVehiclePassageNoticeWithAutopay";
                                            }
                                            else
                                            {
                                                _message_type = "RepostVehiclePassageNoticeWithoutAutopay";
                                                message_type = "RepostVehiclePassageNoticeWithoutAutopay";
                                            }
                                        }
                                    }
                                }
                             }
                        }
                    }

                    _stopwatch.Start();
                    _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP003]Get Account Notification Preference Start");

                    acct_noti_preference noti_Preference = null;
                    noti_Preference = await _notificationService.get_acct_noti_preference(_message_type, (long)_account_id, (bool)_is_perm_acct);
                    _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP004]Get Account Notification Preference End, Duration of milliseconds -> {_stopwatch.Elapsed.TotalMilliseconds}");

                    if (noti_Preference == null)
                    {
                        result.Success = true;
                        result.result = 1;
                        result.display_message = "No notification need to be sent.";

                        _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP005]No notification need to be sent, Time -> {DateTime.Now}, process time(ms) -> {(DateTime.Now - dt).TotalMilliseconds}");
                        return result;
                    }

                    _stopwatch.Restart();

                    var para = await GetMessageQueueData(_account_id, _is_perm_acct, _notifType, _email_address, _mobile_phone, _notification_lang, noti_Preference);
                    para.message_type = _message_type;
                    para.content_values_str = JsonConvert.SerializeObject(_content_values);
                    para.schedule_datetime = _scheduleDT == DateTime.MinValue ? null : _scheduleDT;
                    para.notification_message_id = _notification_message_id;

                    para.mobile_phone = string.IsNullOrEmpty(para.mobile_phone) ? null : _encryption.fnDBSetAESString(para.mobile_phone);
                    para.email_address = string.IsNullOrEmpty(para.email_address) ? null : _encryption.fnDBSetAESString(para.email_address);
                    para.recipient = string.IsNullOrEmpty(para.recipient) ? null : _encryption.fnDBSetAESString(para.recipient);
                    if (!string.IsNullOrWhiteSpace(para.type) && para.type != "ignored" && para.type != "apps")
                    {
                        string sql = $"Insert into {tableName} (" +
                                "bi_user_id, vc_message_type, vc_type, vc_language, vc_email_address, vc_mobile_phone, vc_content_values, i_fail_count, dt_schedule_datetime, ti_status, " +
                                $"b_broadcast, vc_recipient, vc_notification_message_id, b_push_app, vc_push_app_acct_id, b_is_perm_acct, ti_message_category_id, bi_create_user_id, dt_create_dateTime " +                                
                                $") values (" +
                                "@target_user_id, @message_type, @type, @notification_lang, @email_address, @mobile_phone, @content_values_str, 0, @schedule_datetime, 0, " +
                                $"@is_broadcast, @recipient, @notification_message_id, @is_push_app, @push_app_acct_id, @is_perm_acct, @message_category_id, @user_id, sysdate() " +                                
                                $")";

                        await _db.ExecuteAsync(sql, para, false, connection);
                    }                    

                    if (para.is_push_app ?? false) {
                        string sql = $"Insert into {tableName} (" +
                                    "bi_user_id, vc_message_type, vc_type, vc_language, vc_email_address, vc_mobile_phone, vc_content_values, i_fail_count, dt_schedule_datetime, ti_status, " +
                                    $"b_broadcast, vc_recipient, vc_notification_message_id, b_push_app, vc_push_app_acct_id, b_is_perm_acct, ti_message_category_id, bi_create_user_id, dt_create_dateTime " +                                    
                                    $") values (" +
                                    "@target_user_id, @message_type, 'apps', @notification_lang, @email_address, @mobile_phone, @content_values_str, 0, @schedule_datetime, 0, " +
                                    $"@is_broadcast, @recipient, @notification_message_id, @is_push_app, @push_app_acct_id, @is_perm_acct, @message_category_id, @user_id, sysdate() " +                                    
                                    $")";

                        await _db.ExecuteAsync(sql, para, false, connection);
                    }
                    result.Success = true;
                }

                //para.Add("bi_ref_id", _notification_message_id);
                //para.Add("target_user_id", _account_id);
                //para.Add("message_type", _message_type);
                //para.Add("type", _notifType);
                //para.Add("notification_lang", _notification_lang);
                //para.Add("content_values_str", JsonConvert.SerializeObject(_content_values));
                //para.Add("email_address", _email_address);
                //para.Add("mobile_phone", _mobile_phone);
                //para.Add("schedule_datetime", _scheduleDT == DateTime.MinValue ? null : _scheduleDT);
                //para.Add("status", 0);
                //para.Add("is_perm_acct", _is_perm_acct);
                //para.Add("user_id", _user_id);

                //string sql = $"Insert into {tableName} (" +
                //            "bi_ref_id, " +
                //            $"bi_user_id, vc_message_type, vc_type, vc_language, " +
                //            $"vc_content_values, vc_email_address, vc_mobile_phone, i_fail_count, dt_schedule_datetime, " +
                //            $"ti_status, b_is_perm_acct, bi_create_user_id, dt_create_dateTime" +
                //            $") values (" +
                //            "@bi_ref_id, " +
                //            $"@target_user_id, @message_type, @type, @notification_lang, " +
                //            $"@content_values_str, @email_address, @mobile_phone, 0, @schedule_datetime, " +
                //            $"@status, @is_perm_acct, @user_id, sysdate())";


                ////await _db.ExecuteAsync(sql, para);
                //await _db.ExecuteAsync(sql, para, false, connection);                

                //if (string.IsNullOrWhiteSpace(_notification_lang))
                //    _notification_lang = "en";

                //Dictionary<string, Dictionary<string, TemplateDetail>> templateList = new();

                //if (_account_id == -1 && await _notificationService.isBroadcast(_message_type))
                //{
                //    _logger.LogDebug($"notif_msg_id -> {_notification_message_id} - [NP006A]Send Broadcast Start, notifType -> {_notifType} ");

                //    var data = new
                //    {
                //        recipient = "All",
                //        message_title = "",
                //        message_content = "",
                //        notification_message_id = _notification_message_id,
                //        message_type = _message_type
                //    };

                //    List<broadcast_acct_info> acctList = await _notificationService.get_broadcast_acct_info(_notifType);
                //    byte? msgCatID = await _notificationService.get_message_cat_id(_message_type);
                //    string sysNotiDesc = await _notificationService.get_sys_noti_desc(_message_type);

                //    acct_noti_preference bc_noti_Preference = new() { ti_message_category_id = msgCatID, vc_sys_noti_desc_en = sysNotiDesc };

                //    foreach (string lang in acctList.Select(v => v.ti_notification_lang).Distinct().Select(v => GetLanguage(v)).Distinct())
                //    {
                //        Dictionary<string, TemplateDetail> _Dict = new();

                //        _Dict.Add(lang, await _notificationService.get_template_content(new()
                //        {
                //            message_type = _message_type,
                //            notification_type = _notifType
                //        }, _notification_lang));

                //        templateList.Add(_notifType, _Dict);
                //    }

                //    foreach (broadcast_acct_info acct in acctList)
                //    {
                //        await ProcessPushMessage(priority, _account_id, _is_perm_acct, _notifType, _email_address, _mobile_phone, _notification_lang,
                //                                _message_type, _content_values, _notification_message_id, bc_noti_Preference, templateList);
                //    }

                //    _stopwatch.Restart();


                //}
                //else
                //{
                //    Dictionary<string, TemplateDetail> _nDict = new();

                //    _nDict.Add(_notification_lang, await _notificationService.get_template_content(new()
                //    {
                //        message_type = _message_type,
                //        notification_type = _notifType
                //    }, _notification_lang));

                //    templateList.Add(_notifType, _nDict);

                //    Dictionary<string, TemplateDetail> _aDict = new();

                //    _aDict.Add(_notification_lang, await _notificationService.get_template_content(new()
                //    {
                //        message_type = _message_type,
                //        notification_type = "apps"
                //    }, _notification_lang));

                //    templateList.Add("apps", _aDict);

                //    _stopwatch.Start();
                //    _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP003]Get Account Notification Preference Start");

                //    acct_noti_preference noti_Preference = null;
                //    noti_Preference = await _notificationService.get_acct_noti_preference(_message_type, (long)_account_id, (bool)_is_perm_acct);
                //    _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP004]Get Account Notification Preference End, Duration of milliseconds -> {_stopwatch.Elapsed.TotalMilliseconds}");

                //    if (noti_Preference == null)
                //    {
                //        result.Success = true;
                //        result.result = 1;
                //        result.display_message = "No notification need to be sent.";

                //        _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP005]No notification need to be sent, Time -> {DateTime.Now}, process time(ms) -> {(DateTime.Now - dt).TotalMilliseconds}");
                //        return result;
                //    }

                //    _stopwatch.Restart();

                //    await ProcessPushMessage(priority, _account_id, _is_perm_acct, _notifType, _email_address, _mobile_phone, _notification_lang, _message_type, _content_values, _notification_message_id, noti_Preference, templateList);
                //}
            }
            catch (Exception ex)
            {
                _logger.LogError($"SendNotificationV2Async: Error - message_type: -> {message_type}, notification_type -> {notification_type}, " +
                $"schedule_datetime -> {schedule_datetime}, email_address -> {DataMasking.emailMask(email_address)}, mobile_phone -> {DataMasking.phoneNumberMask(mobile_phone)} - Exception (Time -> {DateTime.Now}, Error -> {ex.Message}");
                //throw new ApplicationException(ex.Message);

                result.Success = false;
                result.sys_message = ex.Message;
                result.display_message = "Notification API Error.";
            }
            finally
            {
                _db.CloseIDbConnection(ref connection);

                GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
                GC.Collect();
            }

            return result;
        }

        private async Task<tb_message_queue_with_priority> GetMessageQueueData(long? _account_id, bool? _is_perm_acct, string _notifType, string _email_address,
                                                    string _mobile_phone, string _notification_lang, acct_noti_preference noti_Preference, bool isBoardCast = false)
        {
            tb_message_queue_with_priority result = new tb_message_queue_with_priority();

            try
            {
                string recipientContact = "";
                tb_acct_summary tb_acct = null;

                bool isPushApp = false;
                string pushAppAcctId = "";

                string notifType = _notifType;

                _logger.LogDebug($"account_id -> {_account_id} - [NP006]Get Account Notification Info Start");

                if ((_account_id == 0 && !string.IsNullOrWhiteSpace(_mobile_phone) && notifType.ToLower() == "sms")
                    || (_account_id == 0 && !string.IsNullOrWhiteSpace(_email_address) && notifType.ToLower() == "email")
                    || (_account_id == 0 && notifType.ToLower() == "auto"))
                {
                    bool guidPhoneResult = Guid.TryParse(_mobile_phone, out Guid guidphoneresult);
                    bool guidEmailResult = Guid.TryParse(_email_address, out Guid guidemailresult);
                    string phoneRecipient = _mobile_phone;
                    string emailRecipient = _email_address;

                    if (guidPhoneResult)
                        phoneRecipient = await GetAccountMobilePhoneAsync(_mobile_phone);
                    if (guidEmailResult)
                        emailRecipient = await GetAccountEmailAddressAsync(_email_address);

                    if (notifType.ToLower() == "sms")
                    {
                        recipientContact = phoneRecipient;
                    }
                    else if (notifType.ToLower() == "email")
                    {
                        recipientContact = emailRecipient;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(phoneRecipient) && !string.IsNullOrEmpty(emailRecipient))
                        {
                            // email is priority
                            if (noti_Preference.method_priority == 1)
                            {
                                recipientContact = emailRecipient;
                                notifType = "email";
                            }
                            else
                            {
                                recipientContact = phoneRecipient;
                                notifType = "sms";
                            }
                        }
                        else
                        {
                            if (!string.IsNullOrEmpty(emailRecipient))
                            {
                                recipientContact = emailRecipient;
                                notifType = "email";
                            }
                            else if (!string.IsNullOrEmpty(phoneRecipient))
                            {
                                recipientContact = phoneRecipient;
                                notifType = "sms";
                            }
                            else {
                                notifType = "ignored";
                            }
                        }
                    }
                }
                else
                {
                    tb_acct = await _acctSearchService.GetAccountSummary(_account_id, _is_perm_acct);
                    _logger.LogDebug($"tb_acct -> {JsonConvert.SerializeObject(tb_acct)} - [NP006.1]Get Account Notification Info Object.");
                    _logger.LogDebug($"noti_Preference -> {JsonConvert.SerializeObject(noti_Preference)} - [NP006.2]Get Account Notification Preference Object.");


                    if (notifType.ToLower() == "email" && !string.IsNullOrEmpty(tb_acct.tk_acct_email) && (noti_Preference.b_email == true || isBoardCast))
                    {
                        recipientContact = await GetAccountEmailAddressAsync(tb_acct.tk_acct_email);

                    }
                    else if (notifType.ToLower() == "sms" && !string.IsNullOrEmpty(tb_acct.tk_mobile_phone) && (noti_Preference.b_sms == true || isBoardCast))
                    {
                        recipientContact = await GetAccountMobilePhoneAsync(tb_acct.tk_mobile_phone);

                    }
                    else
                    {
                        // based on acct notification preference setup.
                        if (!isBoardCast)
                        {
                            if (noti_Preference.b_display_on_ui == true)
                            {
                                {
                                    if (!string.IsNullOrEmpty(tb_acct.tk_acct_email) && !string.IsNullOrEmpty(tb_acct.tk_mobile_phone) && noti_Preference.acct_sms == true && noti_Preference.acct_email == true && noti_Preference.b_sms == true && noti_Preference.b_email == true)
                                    {
                                        // email is priority
                                        if (noti_Preference.method_priority == 1)
                                        {
                                            recipientContact = await GetAccountEmailAddressAsync(tb_acct.tk_acct_email);
                                            notifType = "email";
                                        }
                                        else
                                        {
                                            recipientContact = await GetAccountMobilePhoneAsync(tb_acct.tk_mobile_phone);
                                            notifType = "sms";
                                        }
                                    }
                                    else
                                    {
                                        if (!string.IsNullOrEmpty(tb_acct.tk_acct_email) && noti_Preference.acct_email == true && noti_Preference.b_email == true)
                                        {
                                            recipientContact = await GetAccountEmailAddressAsync(tb_acct.tk_acct_email);
                                            notifType = "email";
                                        }
                                        else if (!string.IsNullOrEmpty(tb_acct.tk_mobile_phone) && noti_Preference.acct_sms == true && noti_Preference.b_sms == true)
                                        {
                                            recipientContact = await GetAccountMobilePhoneAsync(tb_acct.tk_mobile_phone);
                                            notifType = "sms";
                                        }
                                        else {
                                            notifType = "ignored";
                                        }
                                    }
                                }


                                if (noti_Preference.acct_apps == true && noti_Preference.b_apps == true)
                                {
                                    isPushApp = true;
                                    pushAppAcctId = tb_acct.vc_acct_id;
                                }
                            }
                            // based on sys notification preference setup.
                            else
                            {
                                if (!string.IsNullOrEmpty(tb_acct.tk_acct_email) && !string.IsNullOrEmpty(tb_acct.tk_mobile_phone) && noti_Preference.b_sms == true && noti_Preference.b_email == true)
                                {
                                    // email is priority
                                    if (noti_Preference.method_priority == 1)
                                    {
                                        recipientContact = await GetAccountEmailAddressAsync(tb_acct.tk_acct_email);
                                        notifType = "email";
                                    }
                                    else
                                    {
                                        recipientContact = await GetAccountMobilePhoneAsync(tb_acct.tk_mobile_phone);
                                        notifType = "sms";
                                    }
                                }
                                else
                                {
                                    if (!string.IsNullOrEmpty(tb_acct.tk_acct_email) && noti_Preference.b_email == true)
                                    {
                                        recipientContact = await GetAccountEmailAddressAsync(tb_acct.tk_acct_email);
                                        notifType = "email";
                                    }
                                    else if (!string.IsNullOrEmpty(tb_acct.tk_mobile_phone) && noti_Preference.b_sms == true)
                                    {
                                        recipientContact = await GetAccountMobilePhoneAsync(tb_acct.tk_mobile_phone);
                                        notifType = "sms";
                                    }
                                    else {
                                        notifType = "ignored";
                                    }
                                }

                                if (noti_Preference.b_apps == true)
                                {
                                    isPushApp = true;
                                    pushAppAcctId = tb_acct.vc_acct_id;
                                }
                            }
                        }
                        else
                        {
                            isPushApp = true;
                            pushAppAcctId = tb_acct.vc_acct_id;
                        }
                    }

                    switch (tb_acct.ti_notification_lang)
                    {
                        default:
                        case 1:
                            _notification_lang = "en";
                            break;
                        case 2:
                            _notification_lang = "tc";
                            break;
                        case 3:
                            _notification_lang = "sc";
                            break;
                    }
                }

                _logger.LogDebug($"account_id -> {_account_id} - [NP007]Get Account Notification Info End, Duration of milliseconds -> {_stopwatch.Elapsed.TotalMilliseconds}");

                if (!string.IsNullOrEmpty(recipientContact) || isPushApp)
                {
                    _stopwatch.Restart();

                    result.target_user_id = _account_id;
                    result.type = notifType;
                    result.notification_lang = _notification_lang;
                    result.email_address = _email_address;
                    result.mobile_phone = _mobile_phone;
                    result.is_perm_acct = _is_perm_acct;
                    result.is_broadcast = isBoardCast;
                    result.vc_noti_type = notifType;
                    result.vc_noti_lang = _notification_lang;

                    if (!string.IsNullOrEmpty(recipientContact))
                    {
                        result.recipient = recipientContact;
                    }

                    if (isPushApp)
                    {
                        result.is_push_app = true;
                        result.push_app_acct_id = pushAppAcctId;
                        result.message_category_id = noti_Preference.ti_message_category_id;
                    }
                }
                else
                {
                    _logger.LogDebug($"account_id -> {_account_id} - [NP014]No recipient is found, notifType -> {notifType}, " +
                        $"recipient -> {(notifType.ToLower() == "email" ? DataMasking.emailMask(recipientContact) : DataMasking.phoneNumberMask(recipientContact))}");
                }
            }
            catch (Exception)
            {

                throw;
            }

            return result;
        }

        private async Task<bool> ProcessPushMessage(Priority priority, long? _account_id, bool? _is_perm_acct, string _notifType, string _email_address, 
                                                    string _mobile_phone, string _notification_lang, string _message_type, string[] _content_values, string _notification_message_id,
                                                    acct_noti_preference noti_Preference, Dictionary<string, Dictionary<string, TemplateDetail>> templateList, bool isBoardCast = false)
        {
            bool retResult = false;

            try
            {
                string recipientContact = "";
                tb_acct_summary tb_acct = null;

                bool isPushApp = false;
                string pushAppAcctId = "";

                string notifType = _notifType;

                _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP006]Get Account Notification Info Start");

                if ((_account_id == 0 && !string.IsNullOrWhiteSpace(_mobile_phone) && notifType.ToLower() == "sms")
                    || (_account_id == 0 && !string.IsNullOrWhiteSpace(_email_address) && notifType.ToLower() == "email")
                    || (_account_id == 0 && notifType.ToLower() == "auto"))
                {
                    bool guidPhoneResult = Guid.TryParse(_mobile_phone, out Guid guidphoneresult);
                    bool guidEmailResult = Guid.TryParse(_email_address, out Guid guidemailresult);
                    string phoneRecipient = _mobile_phone;
                    string emailRecipient = _email_address;

                    if (guidPhoneResult)
                        phoneRecipient = await GetAccountMobilePhoneAsync(_mobile_phone);
                    if (guidEmailResult)
                        emailRecipient = await GetAccountEmailAddressAsync(_email_address);

                    if (notifType.ToLower() == "sms")
                    {
                        recipientContact = phoneRecipient;
                    }
                    else if (notifType.ToLower() == "email")
                    {
                        recipientContact = emailRecipient;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(phoneRecipient) && !string.IsNullOrEmpty(emailRecipient))
                        {
                            // email is priority
                            if (noti_Preference.method_priority == 1)
                            {
                                recipientContact = emailRecipient;
                                notifType = "email";
                            }
                            else
                            {
                                recipientContact = phoneRecipient;
                                notifType = "sms";
                            }
                        }
                        else
                        {
                            if (!string.IsNullOrEmpty(emailRecipient))
                            {
                                recipientContact = emailRecipient;
                                notifType = "email";
                            }
                            else if (!string.IsNullOrEmpty(phoneRecipient))
                            {
                                recipientContact = phoneRecipient;
                                notifType = "sms";
                            }
                        }
                    }
                }
                else
                {
                    tb_acct = await _acctSearchService.GetAccountSummary(_account_id, _is_perm_acct);

                    if (notifType.ToLower() == "email" && !string.IsNullOrEmpty(tb_acct.tk_acct_email) && (noti_Preference.b_email == true || isBoardCast))
                    {
                        recipientContact = await GetAccountEmailAddressAsync(tb_acct.tk_acct_email);

                    }
                    else if (notifType.ToLower() == "sms" && !string.IsNullOrEmpty(tb_acct.tk_mobile_phone) && (noti_Preference.b_sms == true || isBoardCast))
                    {
                        recipientContact = await GetAccountMobilePhoneAsync(tb_acct.tk_mobile_phone);

                    }
                    else
                    {
                        // based on acct notification preference setup.
                        if (!isBoardCast)
                        {
                            if (noti_Preference.b_display_on_ui == true)
                            {
                                {
                                    if (!string.IsNullOrEmpty(tb_acct.tk_acct_email) && !string.IsNullOrEmpty(tb_acct.tk_mobile_phone) && noti_Preference.acct_sms == true && noti_Preference.acct_email == true && noti_Preference.b_sms == true && noti_Preference.b_email == true)
                                    {
                                        // email is priority
                                        if (noti_Preference.method_priority == 1)
                                        {
                                            recipientContact = await GetAccountEmailAddressAsync(tb_acct.tk_acct_email);
                                            notifType = "email";
                                        }
                                        else
                                        {
                                            recipientContact = await GetAccountMobilePhoneAsync(tb_acct.tk_mobile_phone);
                                            notifType = "sms";
                                        }
                                    }
                                    else
                                    {
                                        if (!string.IsNullOrEmpty(tb_acct.tk_acct_email) && noti_Preference.acct_email == true && noti_Preference.b_email == true)
                                        {
                                            recipientContact = await GetAccountEmailAddressAsync(tb_acct.tk_acct_email);
                                            notifType = "email";
                                        }
                                        else if (!string.IsNullOrEmpty(tb_acct.tk_mobile_phone) && noti_Preference.acct_sms == true && noti_Preference.b_sms == true)
                                        {
                                            recipientContact = await GetAccountMobilePhoneAsync(tb_acct.tk_mobile_phone);
                                            notifType = "sms";
                                        }
                                    }
                                }


                                if (noti_Preference.acct_apps == true && noti_Preference.b_apps == true)
                                {
                                    isPushApp = true;
                                    pushAppAcctId = tb_acct.vc_acct_id;
                                }
                            }
                            // based on sys notification preference setup.
                            else
                            {
                                if (!string.IsNullOrEmpty(tb_acct.tk_acct_email) && !string.IsNullOrEmpty(tb_acct.tk_mobile_phone) && noti_Preference.b_sms == true && noti_Preference.b_email == true)
                                {
                                    // email is priority
                                    if (noti_Preference.method_priority == 1)
                                    {
                                        recipientContact = await GetAccountEmailAddressAsync(tb_acct.tk_acct_email);
                                        notifType = "email";
                                    }
                                    else
                                    {
                                        recipientContact = await GetAccountMobilePhoneAsync(tb_acct.tk_mobile_phone);
                                        notifType = "sms";
                                    }
                                }
                                else
                                {
                                    if (!string.IsNullOrEmpty(tb_acct.tk_acct_email) && noti_Preference.b_email == true)
                                    {
                                        recipientContact = await GetAccountEmailAddressAsync(tb_acct.tk_acct_email);
                                        notifType = "email";
                                    }
                                    else if (!string.IsNullOrEmpty(tb_acct.tk_mobile_phone) && noti_Preference.b_sms == true)
                                    {
                                        recipientContact = await GetAccountMobilePhoneAsync(tb_acct.tk_mobile_phone);
                                        notifType = "sms";
                                    }
                                }

                                if (noti_Preference.b_apps == true)
                                {
                                    isPushApp = true;
                                    pushAppAcctId = tb_acct.vc_acct_id;
                                }
                            }
                        }
                         else
                        {
                            isPushApp = true;
                            pushAppAcctId = tb_acct.vc_acct_id;
                        }   
                    }

                    switch (tb_acct.ti_notification_lang)
                    {
                        case 1:
                            _notification_lang = "en";
                            break;
                        case 2:
                            _notification_lang = "tc";
                            break;
                        case 3:
                            _notification_lang = "sc";
                            break;
                    }
                }

                _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP007]Get Account Notification Info End, Duration of milliseconds -> {_stopwatch.Elapsed.TotalMilliseconds}");

                _stopwatch.Restart();
                _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP008]Get Template Content Start, notifType -> {notifType}");
                
                TemplateDetail templateDetail = templateList.Where(f => f.Key == notifType).SelectMany(s => s.Value)
                                                    .Where(s => s.Key == _notification_lang).Select(s => s.Value).FirstOrDefault();

                //TemplateDetail templateDetail = await _notificationService.get_template_content(new()
                //{
                //    message_type = _message_type,
                //    notification_type = notifType                                                            
                //}, _notification_lang);

                _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP009]Get Template Content End, Duration of milliseconds -> {_stopwatch.Elapsed.TotalMilliseconds}");



                if (!string.IsNullOrEmpty(recipientContact))
                {
                    _logger.LogDebug($"recipientContact -> {recipientContact}, templateDetail -> {JsonConvert.SerializeObject(templateDetail)}");

                    _stopwatch.Restart();

                    bool skipPulsar = false;

                    switch ((notifType, priority))
                    {
                        case ("email", Priority.high):
                            if (_SendNotificationConfig.SkipPulsar.toemail.high) skipPulsar = true;
                            break;
                        case ("email", Priority.medium):
                            if (_SendNotificationConfig.SkipPulsar.toemail.medium) skipPulsar = true;
                            break;
                        case ("email", Priority.low):
                            if (_SendNotificationConfig.SkipPulsar.toemail.low) skipPulsar = true;
                            break;
                        case ("sms", Priority.high):
                            if (_SendNotificationConfig.SkipPulsar.tosms.high) skipPulsar = true;
                            break;
                        case ("sms", Priority.medium):
                            if (_SendNotificationConfig.SkipPulsar.tosms.medium) skipPulsar = true;
                            break;
                        case ("sms", Priority.low):
                            if (_SendNotificationConfig.SkipPulsar.tosms.low) skipPulsar = true;
                            break;
                    }

                    if (!skipPulsar)
                    {
                        _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP010]Start to send message to pulsar");

                        var data = new
                        {
                            recipient = recipientContact,
                            message_title = string.IsNullOrEmpty(templateDetail.template_title) ? noti_Preference.vc_sys_noti_desc_en : templateDetail.template_title,
                            message_content = AppendValuesToTemplate(templateDetail.template_content, _content_values, tb_acct),
                            message_type = _message_type,
                            notification_message_id = _notification_message_id
                        };

                        var messageId = await _pulsarProducerService.SendNotiAsync(JsonConvert.SerializeObject(data), notifType, priority);

                        _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP011]send message to pulsar End, messageId -> {messageId}, notification_message_id -> {_notification_message_id}, notifType -> {notifType}, " +
                        $"recipient -> {(notifType.ToLower() == "email" ? DataMasking.emailMask(recipientContact) : DataMasking.phoneNumberMask(recipientContact))}, Duration of milliseconds -> {_stopwatch.Elapsed.TotalMilliseconds}");

                        retResult = true;

                        if (!isPushApp)
                        {
                            await _notificationService.bes_cre_db019_notification_send(_notification_message_id);
                        }
                    }
                    else
                    {
                        _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP012]Start to direct push message");

                        bool pushResult = false;

                        if (notifType == "email")
                        {
                            pushResult = await _pushService.SendEmail(_notification_message_id, recipientContact,
                                                                    string.IsNullOrEmpty(templateDetail.template_title) ? noti_Preference.vc_sys_noti_desc_en : templateDetail.template_title,
                                                                    AppendValuesToTemplate(templateDetail.template_content, _content_values, tb_acct));
                        }
                        else if (notifType == "sms")
                        {
                            pushResult = await _pushService.SendSMS(_notification_message_id, recipientContact,
                                                                    AppendValuesToTemplate(templateDetail.template_content, _content_values, tb_acct), priority);
                        }

                        _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP013]direct push message End, IsSuccess -> {pushResult}, notification_message_id -> {_notification_message_id}, notifType -> {notifType}, " +
                        $"recipient -> {(notifType.ToLower() == "email" ? DataMasking.emailMask(recipientContact) : DataMasking.phoneNumberMask(recipientContact))}, Duration of milliseconds -> {_stopwatch.Elapsed.TotalMilliseconds}");

                        if (!pushResult)
                        {
                            retResult = false;
                        }
                        else
                        {
                            retResult = true;

                            if (!isPushApp)
                            {
                                await _notificationService.bes_cre_db019_notification_send(_notification_message_id);
                            }
                        }
                    }
                }
                else
                {
                    _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP014]No recipient is found, notification_message_id -> {_notification_message_id}, notifType -> {notifType}, " +
                        $"recipient -> {(notifType.ToLower() == "email" ? DataMasking.emailMask(recipientContact) : DataMasking.phoneNumberMask(recipientContact))}");
                }

                if (isPushApp)
                {
                    _stopwatch.Restart();
                    _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP015]Get Template Content Start, notifType -> app");

                    templateDetail = new();
                    templateDetail = templateList.Where(f => f.Key == "apps").SelectMany(s => s.Value)
                                                    .Where(s => s.Key == _notification_lang).Select(s => s.Value).FirstOrDefault();

                    //templateDetail = await _notificationService.get_template_content(new()
                    //{
                    //    message_type = _message_type,
                    //    notification_type = "apps"                      
                    //}, _notification_lang);

                    _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP016]Get Template Content End, Duration of milliseconds -> {_stopwatch.Elapsed.TotalMilliseconds}");


                    bool skipPulsar = false;

                    switch (priority)
                    {
                        case Priority.high:
                            if (_SendNotificationConfig.SkipPulsar.toapps.high) skipPulsar = true;
                            break;

                        case Priority.medium:
                            if (_SendNotificationConfig.SkipPulsar.toapps.medium) skipPulsar = true;
                            break;

                        case Priority.low:
                            if (_SendNotificationConfig.SkipPulsar.toapps.low) skipPulsar = true;
                            break;
                    }

                    if (!skipPulsar)
                    {
                        var pushAppData = new
                        {
                            account_id = _account_id,
                            is_perm_acct = _is_perm_acct,
                            recipient = pushAppAcctId,
                            message_title = string.IsNullOrEmpty(templateDetail.template_title) ? noti_Preference.vc_sys_noti_desc_en : templateDetail.template_title,
                            message_content = AppendValuesToTemplate(templateDetail.template_content, _content_values, tb_acct),
                            message_type = _message_type,
                            notification_message_id = _notification_message_id,
                            message_category_id = noti_Preference.ti_message_category_id
                        };

                        _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP017]Start to send app message to pulsar");

                        var messageId = await _pulsarProducerService.SendNotiAsync(JsonConvert.SerializeObject(pushAppData), notifType, priority);

                        _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP018]send app message to pulsar End, messageId -> {messageId}, notification_message_id -> {_notification_message_id}, " +
                            $"notifType -> app, recipient -> {pushAppAcctId}, Duration of milliseconds -> {_stopwatch.Elapsed.TotalMilliseconds}");

                        retResult = true;

                        await _notificationService.bes_cre_db019_notification_send(_notification_message_id);
                    }
                    else
                    {
                        _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP019]Start to direct push app message");

                        bool pushResult = false;

                        pushResult = await _pushService.SendAppNotify(_account_id, _is_perm_acct, noti_Preference.ti_message_category_id, _notification_message_id, pushAppAcctId,
                                                                    string.IsNullOrEmpty(templateDetail.template_title) ? noti_Preference.vc_sys_noti_desc_en : templateDetail.template_title,
                                                                    AppendValuesToTemplate(templateDetail.template_content, _content_values, tb_acct));

                        _logger.LogDebug($"notif_msg_id -> {_notification_message_id}, account_id -> {_account_id} - [NP013]direct push app message End, IsSuccess -> {pushResult}, notification_message_id -> {_notification_message_id}, notifType -> app, " +
                        $"recipient -> {pushAppAcctId}, Duration of milliseconds -> {_stopwatch.Elapsed.TotalMilliseconds}");

                        if (!pushResult)
                        {
                            retResult = false;
                        }
                        else
                        {
                            retResult = true;

                            await _notificationService.bes_cre_db019_notification_send(_notification_message_id);
                        }
                    }
                }
            }
            catch (Exception)
            {

                throw;
            }

            return retResult;
        }

        public async Task<ProducerResponse> SendNotificationAsync(string message_type, string notification_type, Priority priority, string schedule_datetime, string[] content_values,
            long? target_user_id, bool is_perm_acct, string email_address, string mobile_phone, string notification_lang = "tc")
        {
            DateTime dt = DateTime.Now;
            _logger.LogInformation($"target_user_id -> {target_user_id} - [FCU001]SendNotificationAsync: incoming request, message_type: -> {message_type}, notification_type -> {notification_type}, " +
                $"schedule_datetime -> {schedule_datetime}, email_address -> {DataMasking.emailMask(email_address)}, mobile_phone -> {DataMasking.phoneNumberMask(mobile_phone)}");



            ProducerReturn = new ProducerResponse();
            ProducerReturn.IsApiSuccess = true;
            ProducerReturn.ProducerMsg = "Success";

            //_stopwatch.Start();
            //_logger.LogDebug($"target_user_id -> {target_user_id} - [FCU002]Create pulsar client(if pulsar client is not exists) Start");
            //if (client == null)
            //    client = await new PulsarClientBuilder()
            //             .ServiceUrl(_pulsarServiceUrl)
            //             .KeepAliveInterval(new TimeSpan(0, 0, 20))
            //             //.EnableTls(true)
            //             //.TlsTrustCertificate(new X509Certificate2(ca))
            //             .BuildAsync();
            //_logger.LogDebug($"target_user_id -> {target_user_id} - [FCU003]Create pulsar client(if pulsar client is not exists) End, Duration of milliseconds -> {_stopwatch.Elapsed.TotalMilliseconds}");

            //_stopwatch.Restart();
            //_logger.LogDebug($"target_user_id -> {target_user_id} - [FCU004]Get Producer Start, topic -> {GetTopic(priority)}");

            //bool flagProducerDispose = false;
            //IProducer<string> producer = null;

            //string producerName = $"{Environment.MachineName}_GetTopic(priority)";

            //if (!_producerDict.TryGetValue(producerName, out producer))
            //{
            //    try
            //    {
            //        _logger.LogDebug($"target_user_id -> {target_user_id} - [FCU004A]Create New Producer, topic -> {GetTopic(priority)}");
            //        //producer = await client.NewProducer(Schema.STRING()).SendTimeout(TimeSpan.Zero).Topic(GetTopic(priority)).CreateAsync();

            //        producer = await client.NewProducer(Schema.STRING())
            //           .SendTimeout(TimeSpan.Zero)
            //           .Topic(GetTopic(priority))
            //           .BlockIfQueueFull(true)
            //           .EnableBatching(false)
            //           .EnableChunking(false)
            //           .SendTimeout(TimeSpan.FromSeconds(60))
            //           .CreateAsync();

            //        if (_producerDict.TryAdd(producerName, producer))
            //        {
            //            if (_cache != null) _cache.Set("ProducerDict", _producerDict);
            //        }
            //        else
            //        {
            //            flagProducerDispose = true;
            //        }
            //    }
            //    catch (IndexOutOfRangeException)
            //    {
            //        flagProducerDispose = true;
            //        _logger.LogDebug($"target_user_id -> {target_user_id} - [FCU004B]Create New Producer with ignore same key on dictionary add, topic -> {GetTopic(priority)}");
            //    }
            //}

            //_logger.LogDebug($"target_user_id -> {target_user_id} - [FCU005]Get Producer End, ProducerID -> {producer.ProducerId}, Duration of milliseconds -> {_stopwatch.Elapsed.TotalMilliseconds}");

            NotiMsgReceiveRequest req = new NotiMsgReceiveRequest();
            req.user_id = 1;
            req.message_type = message_type;
            req.notification_type = notification_type;
            req.notification_lang = notification_lang;
            req.schedule_datetime = schedule_datetime;
            req.target_user_id = target_user_id;
            req.is_perm_acct = is_perm_acct;
            req.content_values = content_values;
            req.email_address = email_address;
            req.mobile_phone = mobile_phone;


            //_logger.LogDebug($"target_user_id -> {target_user_id} - [FCU006]Produce Messages Start, ProducerID -> {producer.ProducerId}");
            //await ProduceMessages(producer, JsonConvert.SerializeObject(req));
            //_logger.LogDebug($"target_user_id -> {target_user_id} - [FCU007]Produce Messages End, Duration of milliseconds -> {_stopwatch.Elapsed.TotalMilliseconds}");

            //if (flagProducerDispose)
            //{
            //    _logger.LogDebug($"target_user_id -> {target_user_id} - [FCU007B]Dispose additional producer, ProducerID -> {producer.ProducerId}");
            //    await producer.DisposeAsync();
            //}

            _logger.LogDebug($"target_user_id -> {target_user_id} - [FCU006]Produce Messages Start");

            var messageId = await _pulsarProducerService.SendNotiRcvAsync(JsonConvert.SerializeObject(req), priority);

            _logger.LogInformation($"target_user_id -> {target_user_id} - [FCU008]SendNotificationAsync: outgoing, messageId -> {messageId}, Time -> {DateTime.Now}, process time(ms) -> {(DateTime.Now - dt).TotalMilliseconds}");
            return ProducerReturn;
        }

        public static async Task ProduceMessages(IProducer<string> producer, string request)
        {
            try
            {
                _ = await producer.SendAsync(request);
            }
            catch (Exception e)
            {
                ProducerReturn.IsApiSuccess = false;
                ProducerReturn.ProducerMsg = e.Message;
                Console.WriteLine(e.Message);
            }
        }

        private string GetTopic(Priority priority)
        {
            string topic = string.Empty;

            switch (priority)
            {
                case Priority.high:
                    topic = _topicHighPriority;
                    break;
                case Priority.medium:
                    topic = _topicMediumPriority;
                    break;
                case Priority.low:
                    topic = _topicLowPriority;
                    break;
                default:
                    break;
            }
            return topic;
        }

        private async Task<string> getNextDocNoAsync(string docType)
        {
            var para = new DynamicParameters();
            para.Add(name: "@in_doc_type", value: docType, dbType: DbType.String, direction: ParameterDirection.Input);
            para.Add(name: "@out_doc_no", dbType: DbType.String, direction: ParameterDirection.Output);
            para.Add(name: "@out_affected", dbType: DbType.Int64, direction: ParameterDirection.Output);
            para.Add(name: "@out_flag", dbType: DbType.Int32, direction: ParameterDirection.Output);

            try
            {
                await _db.ExecuteAsync(sql: "sp_tm001_get_next_doc_no", parameters: para, true, connection);

                string docNo = para.Get<string>("out_doc_no");
                long affectedRow = para.Get<long>("out_affected");
                int out_flag = para.Get<int>("out_flag");

                if (out_flag == -1 && affectedRow == 0)
                {
                    //Concurrency Conflict
                    return await getNextDocNoAsync(docType);
                }
                else
                {
                    return docNo;
                }

            }
            catch (Exception)
            {
                throw new Exception("Document Type Not Exists");
            }
        }

        private async Task<string> GetAccountEmailAddressAsync(string tk_acct_email)
        {
            string recipientContact = "";
            var token_response = await _tokenService.TokenRetrieve(new TokenRetrieveRequest()
            {
                token_type = sensitiveField.email,
                uuid_list = new List<string>() { tk_acct_email }
            });
            if (token_response.result > 0 && token_response.token_list.Count > 0)
            {
                recipientContact = token_response.token_list.First().token_value;
            }
            return recipientContact;
        }

        private async Task<string> GetAccountMobilePhoneAsync(string tk_mobile_phone)
        {
            string recipientContact = "";
            var token_response = await _tokenService.TokenRetrieve(new TokenRetrieveRequest()
            {
                token_type = sensitiveField.phone,
                uuid_list = new List<string>() { tk_mobile_phone }
            });
            if (token_response.result > 0 && token_response.token_list.Count > 0)
            {
                recipientContact = token_response.token_list.First().token_value;
            }
            return recipientContact;
        }

        private string AppendValuesToTemplate(string template, string[] values, tb_acct_summary tb_acct)
        {
            string content = template;

            //int num = 1;
            //string dynamicVariable = "{{variable" + num + "}}";
            //while (content.Contains(dynamicVariable))
            //{
            //    content = content.Replace(dynamicVariable, num > values.Length ? string.Empty : values[num - 1]);
            //    dynamicVariable = "{{variable" + (++num) + "}}";
            //}
            for (int i = 0; i < values.Length; i++)
            {
                string dynamicVariable = "{{variable" + (i + 1) + "}}";
                if (content.Contains(dynamicVariable))
                {
                    content = content.Replace(dynamicVariable, values[i]);
                }
            }

            if (content.Contains("{{validityperiod}}"))
                content = content.Replace("{{validityperiod}}", "15");

            if (content.Contains("{{otpvalue}}"))
                content = content.Replace("{{otpvalue}}", new Random().Next(1000000).ToString("000000"));

            if (content.Contains("{{display_name}}") && tb_acct != null)
            {
                if (!string.IsNullOrEmpty(tb_acct.vc_display_name))
                    content = content.Replace("{{display_name}}", tb_acct.vc_display_name);
                else
                    content = content.Replace("{{display_name}}", (tb_acct.vc_last_name ?? "") + (tb_acct.vc_first_name_masked ?? ""));
            }

            if (content.Contains("{{account_no}}") && tb_acct != null)
                content = content.Replace("{{account_no}}", tb_acct.vc_acct_id);

            if (content.Contains("{{biz_name}}") && tb_acct != null)
                content = content.Replace("{{biz_name}}", tb_acct.vc_biz_name_desc_masked);

            return content;
        }

        private string GetLanguage(int? lang)
        {
            string result = "";

            switch (lang)
            {
                case 2:
                    result = "tc";
                    break;
                case 3:
                    result = "sc";
                    break;
                case 0:
                case 1:
                default:
                    result = "tc";
                    break;
            }

            return result;
        }
    }
}