﻿using AppLensV3.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Net;
using System.Threading.Tasks;
using AppLensV3.Helpers;
using Microsoft.AspNetCore.Authorization;
using Microsoft.IdentityModel.Xml;

namespace AppLensV3.Controllers
{
    [Route("api/openai")]
    [Produces("application/json")]
    [Authorize(Policy = "ApplensAccess")]
    public class OpenAIController : Controller
    {
        private IOpenAIService _openAIService;
        private ILogger<OpenAIController> _logger;
        public OpenAIController(IOpenAIService openAIService, ILogger<OpenAIController> logger)
        {
            _logger = logger;
            _openAIService = openAIService;
        }

        [HttpGet("enabled")]
        public async Task<IActionResult> IsEnabled()
        {
            return Ok(_openAIService.IsEnabled());
        }

        [HttpPost("runTextCompletion")]
        public async Task<IActionResult> RunTextCompletion([FromBody] CompletionModel completionModel)
        {
            if (!_openAIService.IsEnabled())
            {
                return StatusCode(422, "Text Completion Feature is currently disabled.");
            }

            if (completionModel == null || completionModel.Payload == null)
            {
                return BadRequest("Please provide completion payload in the request body");
            }

            try
            {
                // Check if client has requested cache to be disabled
                bool cachingEnabled = bool.TryParse(GetHeaderOrDefault(Request.Headers, HeaderConstants.OpenAICacheHeader, true.ToString()), out var cacheHeader)? cacheHeader: true;
                var response = await _openAIService.RunTextCompletion(completionModel, cachingEnabled);
                if (response.IsSuccessStatusCode)
                {
                    var result = await response.Content.ReadAsStringAsync();
                    return Ok(JsonConvert.DeserializeObject(result));
                }
                else
                {
                    _logger.LogError($"OpenAICallError: {response.StatusCode} {await response.Content.ReadAsStringAsync()}");
                    switch (response.StatusCode)
                    {
                        case HttpStatusCode.Unauthorized:
                        case HttpStatusCode.Forbidden:
                        case HttpStatusCode.NotFound:
                        case HttpStatusCode.InternalServerError:
                            return new StatusCodeResult(500);
                        case HttpStatusCode.BadRequest:
                            return BadRequest("Malformed request");
                        default:
                            return new StatusCodeResult((int)response.StatusCode);
                    }
                }
            }
            catch(Exception ex)
            {
                _logger.LogError(ex.ToString());
                return StatusCode(500, "An error occurred while processing the text completion request.");
            }
        }

        private static string GetHeaderOrDefault(IHeaderDictionary headers, string headerName, string defaultValue = "")
        {
            if (headers == null || headerName == null)
            {
                return defaultValue;
            }

            if (headers.TryGetValue(headerName, out var outValue))
            {
                return outValue;
            }

            return defaultValue;
        }
    }
}
