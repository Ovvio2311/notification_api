using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Newtonsoft.Json;

using Pulsar.Client.Api;
using Pulsar.Client.Common;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using FFTS.IOptions;
using Microsoft.Extensions.Caching.Memory;
using System.Collections.Concurrent;
using notification_api.Models;

namespace notification_api.Services
{
    public class PulsarProducerService
    {
        private readonly ILogger _logger;
        private readonly IConfiguration _configuration;

        private readonly SendNotification _SendNotificationConfig;

        private PulsarClient _client;

        private static ConcurrentDictionary<string, IProducer<string>> _producerDic = new();

        private readonly IMemoryCache _cache;

        public PulsarProducerService(ILogger<PulsarProducerService> logger, IConfiguration configuration, IMemoryCache cache, PulsarClient client)
        {
            _logger = logger;
            _configuration = configuration;

            _SendNotificationConfig = _configuration.GetSection("SendNotification").Get<SendNotification>();

            _cache = cache;

            _client = client;

            if (_cache != null) 
            {
                if (!_cache.TryGetValue("ProducerDict", out _producerDic)) _producerDic = new ConcurrentDictionary<string, IProducer<string>>();
            }            

            ConfigPulsarLogger(_logger);
        }
       
        public async Task<MessageId> SendNotiAsync(string message, string notifType, Priority priority)
        {
            _logger.LogDebug($"Start PulsarProducerService.SendNotiAsync");

            IProducer<string> producer;

            string producerName = $"{Environment.MachineName}_SNA";
            string topic = GetSendTopic(notifType, priority);
            
            producer = await getProducerAsync(producerName, topic);

            var pulsarMsgId = await producer.SendAsync(message);

            _logger.LogDebug($"End PulsarProducerService.SendNotiAsync, Message Entry Id : {pulsarMsgId.EntryId}");
            return pulsarMsgId;
        }

        public async Task<MessageId> SendNotiRcvAsync(string message, Priority priority)
        {
            _logger.LogDebug($"Start PulsarProducerService.SendNotiRcvAsync");

            IProducer<string> producer;

            string producerName = $"{Environment.MachineName}_SNR";
            string topic = GetTopic(priority);

            producer = await getProducerAsync(producerName, topic);

            var pulsarMsgId = await producer.SendAsync(message);

            _logger.LogDebug($"End PulsarProducerService.SendNotiRcvAsync, Message Entry Id : {pulsarMsgId.EntryId}");
            return pulsarMsgId;
        }


        private async Task<IProducer<string>> getProducerAsync(string producerName, string topic)
        {
            try
            {                
                if (_producerDic.TryGetValue(producerName, out IProducer<string> producer))
                {
                    if (producer.Topic != topic)
                    {
                        throw new Exception($"Producer Topic Conflict, producer name:{producerName}, producer topic:{producer.Topic}, target topic:{topic}");
                    }

                    return producer;
                }
                else
                {
                    var client = await getPulsarClientAsync();
                    var newProducer = await client.NewProducer(Schema.STRING())
                       //.ProducerName(producerName)
                       .Topic(topic)
                       .BlockIfQueueFull(true)
                       .EnableBatching(false)
                       .EnableChunking(false)
                       .SendTimeout(TimeSpan.FromSeconds(_SendNotificationConfig.sendTimeoutSecond))
                       .CreateAsync();

                    if (_producerDic.TryAdd(producerName, newProducer))
                    {
                        if (_cache != null) _cache.Set("ProducerDict", _producerDic);
                    }

                    return newProducer;
                }                
            }
            catch (Exception)
            {

                throw;
            }            
        }

        private async Task<PulsarClient> getPulsarClientAsync()
        {
            if (_client == null)
            {
                _logger.LogDebug("Start PulsarProducerService.getPulsarClientAsync");
                _client = await new PulsarClientBuilder()
                    .ServiceUrl(_SendNotificationConfig.PulsarUri)
                    .OperationTimeout(TimeSpan.FromSeconds(_SendNotificationConfig.sendTimeoutSecond))
                    .BuildAsync();

                _logger.LogDebug("End PulsarProducerService.getPulsarClientAsync");
            }

            return _client;
        }

        private void ConfigPulsarLogger(ILogger logger)
        {
            if (_SendNotificationConfig.EnablePulsarClientLogger)
            {
                PulsarClient.Logger = logger;
            }
        }

        private string GetTopic(Priority priority)
        {
            string topic = string.Empty;

            switch (priority)
            {
                case Priority.high:
                    topic = _SendNotificationConfig.Topics.HighPriority;
                    break;
                case Priority.medium:
                    topic = _SendNotificationConfig.Topics.MediumPriority;
                    break;
                case Priority.low:
                    topic = _SendNotificationConfig.Topics.LowPriority;
                    break;
                default:
                    break;
            }
            return topic;
        }

        private string GetSendTopic(string notifType, Priority priority)
        {
            string topic = string.Empty;

            switch ((notifType, priority))
            {
                case ("email", Priority.high):
                    topic = _SendNotificationConfig.SendTopics.email.high;
                    break;
                case ("email", Priority.medium):
                    topic = _SendNotificationConfig.SendTopics.email.medium;
                    break;
                case ("email", Priority.low):
                    topic = _SendNotificationConfig.SendTopics.email.low;
                    break;
                case ("sms", Priority.high):
                    topic = _SendNotificationConfig.SendTopics.sms.high;
                    break;
                case ("sms", Priority.medium):
                    topic = _SendNotificationConfig.SendTopics.sms.medium;
                    break;
                case ("sms", Priority.low):
                    topic = _SendNotificationConfig.SendTopics.sms.low;
                    break;
                case ("apps", Priority.high):
                    topic = _SendNotificationConfig.SendTopics.apps.high;
                    break;
                case ("apps", Priority.medium):
                    topic = _SendNotificationConfig.SendTopics.apps.medium;
                    break;
                case ("apps", Priority.low):
                    topic = _SendNotificationConfig.SendTopics.apps.low;
                    break;
                default:
                    break;
            }
            return topic;
        }
    }
}
