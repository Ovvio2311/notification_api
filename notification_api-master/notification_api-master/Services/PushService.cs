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
using System.Security.Cryptography.X509Certificates;
using System.Net.Http;
using System.Net.Http.Json;
using System.Net.Mail;
using FFTS.IOptions;

namespace notification_api.Services
{
    public class PushService
    {
        private readonly IConfiguration _configuration;

        private Db _db;
        private string connstring;
        private DataEncryption _encryption;
        private NotificationService _notificationService;

        private IDbConnection _conn = null;
        private readonly bool initDBConn = false;

        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger<PushService> _logger;

        private readonly EmailConfig _EmailConfig;

        public PushService(IConfiguration configuration, ILoggerFactory loggerFactory, ILogger<PushService> logger = null, IDbConnection conn = null)
        {
            _configuration = configuration;
            _db = new Db(_configuration);

            _loggerFactory = loggerFactory;
            _logger = logger;

            _encryption = new DataEncryption();
            
            _EmailConfig = _configuration.GetSection("EmailConfig").Get<EmailConfig>();

            if (conn == null || conn.State != ConnectionState.Open)
            {
                _conn = _db.getIDbOpenConnection();
                initDBConn = true;
            }
            else
            {
                _conn = conn;
            }

            _notificationService = new NotificationService(configuration, _loggerFactory, _loggerFactory.CreateLogger<NotificationService>(), _conn);

        }

        public async Task<bool> SendSMS(string notification_message_id, string recipient, string message_content, Priority priority)
        {
            bool result = false;

            try
            {
                int sms_priority = 1;

                if (priority == Priority.medium)
                    sms_priority = 5;
                else if (priority == Priority.low)
                    sms_priority = 10;

                SMSPayload newPayload = new SMSPayload();
                newPayload.source_id = 1;
                SMSJob newJob = new SMSJob();
                newJob.phone_no = "852" + recipient;
                newJob.subject = "";
                newJob.message = message_content;
                newJob.priority = sms_priority;
                newJob.schedule_start = "";
                newPayload.jobs = new List<SMSJob>();
                newPayload.jobs.Add(newJob);

                DateTime dt = DateTime.Now;
                _logger.LogDebug($"Send SMS Start, notif_msg_id -> {notification_message_id}, recipient -> {DataMasking.phoneNumberMask(recipient)}");


                var cert = X509Certificate2.CreateFromPemFile(_configuration["ClientCertConfig:Path"], _configuration["ClientCertConfig:KeyPath"]);
                cert = new X509Certificate2(cert.Export(X509ContentType.Pfx));

                var handler = new HttpClientHandler();

                handler.ClientCertificates.Add(cert);
                var _httpClient = new HttpClient(handler);

                BaseResponse apiResult = new();

                string requestURI = $"{_configuration["NotificationResultAPIHost"]}NotificationSend/bes_cre_ia028_create_sms_job";
                HttpResponseMessage res = await _httpClient.PostAsync(requestURI, JsonContent.Create(newPayload));

                if (res.IsSuccessStatusCode)
                {
                    apiResult = JsonConvert.DeserializeObject<BaseResponse>(res.Content.ReadAsStringAsync().Result);

                    result = true;
                }
                else
                {
                    apiResult.sys_message = res.ReasonPhrase;

                    result = false;
                }

                _logger.LogDebug($"Send SMS End, IsSuccess -> {apiResult.result != 0}, Result Message -> {apiResult.sys_message}, Duration of milliseconds -> {(DateTime.Now - dt).TotalMilliseconds}");

                //_logger.LogDebug($"{_randomPrgmID} - {consumerName} - {_consumerTypeName}: id={msgID} -> [{threadID}][{tid}][004B]NotiSMS - Send SMS End");

                
            }
            catch (Exception ex)
            {
                throw;
            }

            return result;
        }

        public async Task<bool> SendEmail(string notification_message_id, string recipient, string message_title, string message_content)
        {
            bool result = false;

            try
            {
                if (WhitelistValidation(recipient))
                {
                    MailMessage mail = new MailMessage(_EmailConfig.SenderEmail, recipient);
                    SmtpClient client = new SmtpClient();

                    client.Host = _EmailConfig.SMTPServer;
                    client.Port = 25;
                    client.UseDefaultCredentials = false;
                    client.DeliveryMethod = SmtpDeliveryMethod.Network;
                    mail.BodyEncoding = System.Text.UTF8Encoding.UTF8;
                    mail.IsBodyHtml = true;
                    mail.Subject = message_title;
                    mail.Body = "<html>\r\n"
                                        + "  <head>\r\n"
                                        + "  </head>\r\n"
                                        + "  <body>\r\n"
                                        + message_content
                                        + "</body></html>\r\n";

                    client.Send(mail);
                }
                else
                {
                    _logger.LogDebug($"[004C]NotiEmail - Send Email by passing (not in whitelist)");
                }                    

            }
            catch (Exception)
            {

                throw;
            }

            return result;
        }

        public async Task<bool> SendAppNotify(long? account_id, bool? is_perm_acct, byte? message_category_id, string notification_message_id, string recipient, string message_title, string message_content)
        {
            bool result = false;

            try
            {
                var addAcctnotiRequest = new AddAcctNotificationRequest()
                {
                    account_id = account_id,
                    is_perm_acct = is_perm_acct,
                    message_category_id = message_category_id,
                    message_content = message_content,
                    message_title = message_title
                };

                _logger.LogInformation(@"Add acct notification: " + JsonConvert.SerializeObject(addAcctnotiRequest));

                await _notificationService.bes_cre_db020_add_acct_notification(addAcctnotiRequest);

                PushJobOnePayload newPayload = new PushJobOnePayload();
                string titleMessage = message_content;
                if (!string.IsNullOrEmpty(titleMessage))
                    titleMessage = titleMessage.Replace("\n\n", "\n");

                if (!string.IsNullOrEmpty(message_title))
                    titleMessage = message_title + Environment.NewLine + titleMessage;

                newPayload.message = titleMessage;
                newPayload.source_id = 1;
                PushJobOneJob newJob = new PushJobOneJob();
                newJob.fft_account_id = recipient;
                newJob.priority = 1;
                newPayload.jobs = new List<PushJobOneJob>();
                newPayload.jobs.Add(newJob);

                DateTime dt = DateTime.Now;
                _logger.LogDebug($"Send App Notification Start, notif_msg_id -> {notification_message_id}, recipient -> {recipient}");
                //BaseResponseObject res = await _notiResultService.PushJob(newPayload);


                var cert = X509Certificate2.CreateFromPemFile(_configuration["ClientCertConfig:Path"], _configuration["ClientCertConfig:KeyPath"]);
                cert = new X509Certificate2(cert.Export(X509ContentType.Pfx));

                var handler = new HttpClientHandler();

                handler.ClientCertificates.Add(cert);
                var _httpClient = new HttpClient(handler);

                BaseResponse apiResult = new();

                string requestURI = $"{_configuration["NotificationResultAPIHost"]}NotificationSend/bes_cre_ia026_create_push_job_one_to_many";
                HttpResponseMessage res = await _httpClient.PostAsync(requestURI, JsonContent.Create(newPayload));

                if (res.IsSuccessStatusCode)
                {
                    apiResult = JsonConvert.DeserializeObject<BaseResponse>(res.Content.ReadAsStringAsync().Result);

                    result = true;
                }
                else
                {
                    apiResult.sys_message = res.ReasonPhrase;

                    result = false;
                }


                _logger.LogDebug($"Send App Notification End, {apiResult.result != 0}, Result Message -> {apiResult.sys_message}, Duration of milliseconds -> {(DateTime.Now - dt).TotalMilliseconds}");

            }
            catch (Exception)
            {

                throw;
            }

            return result;
        }

        private bool WhitelistValidation(string email)
        {
            bool canSendEmail = false;

            if (_EmailConfig.ApplyWhitelist)
            {
                _logger.LogDebug($"[003F]NotiEmail - Apply Whitelist -> true");

                if (_EmailConfig.WhiteList.Contains(email.ToLower()))
                {
                    canSendEmail = true;
                }
                else
                {
                    canSendEmail = false;

                    _logger.LogDebug($"[003G]NotiEmail - recipient -> {DataMasking.emailMask(email)} not in whitelist");
                }
            }
            else
            {
                canSendEmail = true;
            }

            return canSendEmail;
        }
    }

    public class SMSPayload
    {
        public int source_id { get; set; }
        public List<SMSJob> jobs { get; set; }
    }

    public class SMSJob
    {
        public string phone_no { get; set; }
        public string subject { get; set; }
        public string message { get; set; }
        public int priority { get; set; }
        public string schedule_start { get; set; }
    }
    public class PushJobOnePayload
    {
        public string message { get; set; }
        public string module { get; set; }
        public string category { get; set; }
        public int source_id { get; set; }
        public List<PushJobOneJob> jobs { get; set; }
    }

    public class PushJobOneJob
    {
        public string fft_account_id { get; set; }
        public int priority { get; set; }
        public string schedule_start { get; set; }
    }
}
