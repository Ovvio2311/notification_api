using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using notification_api.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace notification_api.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class NotificationApiController : ControllerBase
    {
        private NotificationUtils _NotificationUtils;
        private ILogger<NotificationApiController> _logger;
        private IConfiguration _configuration;

        private readonly bool _EnablePulsar = false;

        public NotificationApiController(ILogger<NotificationApiController> logger, IConfiguration configuration, NotificationUtils NotificationUtils)
        {
            _logger = logger;
            _NotificationUtils = NotificationUtils;
            _configuration = configuration;

            _EnablePulsar = Convert.ToBoolean(configuration["SendNotification:Enable"] ?? "false");
        }

        [HttpPost]
        public async Task<ProducerResponse> notification_api001_SendNotification(sendNotificationObj req)
        {
            return await _NotificationUtils.SendNotificationAsync(req.message_type, req.notification_type,
                 req.priority, req.schedule_datetime, req.content_values, req.target_user_id, req.is_perm_acct, req.email_address, req.mobile_phone, req.notification_lang);
        }

        [HttpPost]
        [Route("notification_api002_SendNotificationV2")]
        public async Task<NotiApiResult> notification_api002_SendNotificationV2(sendNotificationObj req)
        {
            return await _NotificationUtils.SendNotificationV2Async(req.message_type, req.notification_type,
                req.priority, req.schedule_datetime, req.content_values, req.target_user_id, req.is_perm_acct, req.email_address, req.mobile_phone, req.notification_lang);
        }
    }
}
