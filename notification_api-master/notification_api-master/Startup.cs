using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.OpenApi.Models;
using notification_api.Services;
using Pulsar.Client.Api;

using Serilog;

using System;

namespace notification_api
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public async void ConfigureServices(IServiceCollection services)
        {

            services.AddControllers();
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo { Title = "notification_api", Version = "v1" });
            });
           
            services.AddSingleton<PulsarProducerService>();
            services.AddScoped<NotificationUtils>();

            var pulsarClient = await new PulsarClientBuilder()
                        .ServiceUrl(Configuration.GetValue<string>("SendNotification:PulsarUri"))
                        .KeepAliveInterval(new TimeSpan(0, 0, 20))
                        //.EnableTls(true)
                        //.TlsTrustCertificate(new X509Certificate2(ca))
                        .BuildAsync();

            services.AddSingleton(pulsarClient);

            services.AddMemoryCache();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsStaging() == false && env.IsProduction() == false)
            {
                app.UseDeveloperExceptionPage();
                app.UseSwagger();
                app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "notification_api v1"));
            }

            //app.UseHttpsRedirection();

            app.UseRouting();

            app.UseSerilogRequestLogging();

            //app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }
    }
}
