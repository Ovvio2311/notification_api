using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using System.Net.Http;
using Newtonsoft.Json;
using FFTS.InternalCommonModels.APIRequestObjects.TokenizationAPI;
using System.Net.Http.Json;

namespace notification_api 
{
    public class TokenizationService
    {
        private readonly ILogger<TokenizationService> _logger;
        private readonly IConfiguration _configuration;
        private readonly int HttpClientTimeOut;

        private HttpClient _httpClient;

        public TokenizationService(ILogger<TokenizationService> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;

            _httpClient = new();
            _httpClient.BaseAddress = new Uri(_configuration["TokenizationAPIHost"]);

            HttpClientTimeOut = _configuration.GetValue<int>("HttpClientTimeOut");
            _httpClient.Timeout = TimeSpan.FromSeconds(HttpClientTimeOut);
        }

        public async Task<TokenCreateResponse> TokenCreate(TokenCreateRequest request)
        {
            try
            {
                var apiCallUrl =  "/Tokenization/bes_cre_ia001_token_create";
                _logger.LogInformation(apiCallUrl);

                HttpResponseMessage response = await _httpClient.PostAsync(apiCallUrl, JsonContent.Create(request));
                TokenCreateResponse apiCallResult = JsonConvert.DeserializeObject<TokenCreateResponse>(response.Content.ReadAsStringAsync().Result);
                return apiCallResult;
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task<TokenRetrieveResponse> TokenRetrieve(TokenRetrieveRequest request)
        {
            try
            {
                var apiCallUrl = "/Tokenization/bes_cre_ia002_token_value_retrieve";
                _logger.LogInformation(apiCallUrl);

                HttpResponseMessage response = await _httpClient.PostAsync(apiCallUrl, JsonContent.Create(request));
                TokenRetrieveResponse apiCallResult = JsonConvert.DeserializeObject<TokenRetrieveResponse>(response.Content.ReadAsStringAsync().Result);
                return apiCallResult;
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task<TokenRevokeResponse> TokenRevoke(TokenRevokeRequest request)
        {
            try
            {
                var apiCallUrl = "/Tokenization/bes_cre_ia003_token_revoke";
                _logger.LogInformation(apiCallUrl);

                HttpResponseMessage response = await _httpClient.PostAsync(apiCallUrl, JsonContent.Create(request));
                TokenRevokeResponse apiCallResult = JsonConvert.DeserializeObject<TokenRevokeResponse>(response.Content.ReadAsStringAsync().Result);
                return apiCallResult;
            }
            catch (Exception)
            {
                throw;
            }
        }
    }
}
