# Azure Cost Model

## Scope and pricing date

This estimate models a personal portfolio deployment in Azure Japan East using
public pay-as-you-go retail prices observed on 2026-09-01. It is not a quote.
Enterprise agreements, taxes, currency conversion, preview billing, free grants,
and future price changes can alter the result.

The estimate excludes developer labor, outbound internet transfer, private
endpoints, premium support, and production redundancy.

## Reference workload

| Assumption | Public demo | Stream showcase | Continuous stream |
|---|---:|---:|---:|
| API traffic | Fewer than 10,000 requests/month | Same | Fewer than 50,000 requests/month |
| Batch jobs | 5 minutes/hour at 0.5 vCPU and 1 GiB | Same | Same |
| Streaming resources | Not provisioned | Event Hubs provisioned; workers run on demand | Event Hubs plus one 0.25 vCPU and 0.5 GiB worker active 24/7 |
| Stored data | 25 GiB Hot LRS | 50 GiB Hot LRS | 100 GiB Hot LRS |
| Model usage | 5M input and 1M output tokens/month | Same | 20M input and 4M output tokens/month |
| Log ingestion | Kept within the free allowance | Kept within or near the free allowance | 5-10 GiB/month after sampling and redaction |

## Monthly estimate

| Resource | Public demo | Stream showcase | Continuous stream | Notes |
|---|---:|---:|---:|---|
| Static Web Apps Free | $0 | $0 | $0 | No SLA; sufficient for a portfolio site |
| Container Apps | $0-5 | $0-8 | $14-30 | Public and showcase workloads should fit mostly within the monthly free grant |
| Event Hubs Basic, 1 TU | $0 | $10.95 | $10.95 | $0.015/hour for 730 hours, plus low ingress-event charges |
| Container Registry Basic | $5.07 | $5.07 | $5.07 | $0.1666/day |
| ADLS Gen2 Hot LRS | $0.50-1 | $1-2 | $2-4 | First-tier storage is about $0.02/GiB-month plus operations |
| Application Insights | $0-5 | $0-8 | $0-20 | Depends on sampled log volume; Japan East analytics ingestion is $3.34/GiB after allowances |
| Foundry model calls | About $3.60 | About $3.60 | About $14.40 | Example GPT-4.1 mini global rates: $0.40/M input and $1.60/M output tokens |
| **Estimated total** | **$10-20/month** | **$20-40/month** | **$45-90/month** | Rounded planning range |

The low end assumes the Azure subscription still has the relevant Container Apps
and monitoring free grants available. Free grants are shared at subscription
scope and may already be consumed by other projects. The agent has no separate
hosted-runtime charge because FastAPI and LangGraph share one Container App.

### Container Apps environment meter caveat

On 2026-09-01, the Azure Retail Prices API also returned a Japan East
`Environment Management Hour` meter at $0.145/hour. The current Container Apps
pricing page and FAQ describe ordinary Consumption billing in terms of CPU,
memory, and requests, and do not map this meter to a default consumption-only
environment. It is therefore excluded from the table above.

Before the first deployment, confirm the generated environment configuration in
the Azure Pricing Calculator and the subscription cost analysis. If this meter
applies to the selected configuration, it adds about $105.85 per 730-hour month,
raising every mode by the same amount. The Terraform design must avoid optional
environment features that activate this meter unless they provide a required
capability.

## Container Apps calculation

Azure Container Apps Consumption currently includes 180,000 vCPU-seconds,
360,000 GiB-seconds, and two million requests per subscription each month.
Japan East active rates used here are $0.000024 per vCPU-second and $0.000003 per
GiB-second.

For one continuously active 0.25 vCPU and 0.5 GiB worker:

```text
vCPU:   (0.25 * 2,628,000 - 180,000) * $0.000024 = $11.45
memory: (0.50 * 2,628,000 - 360,000) * $0.000003 =  $2.86
total:                                                     $14.31
```

Other Container Apps share the same free grant, so the live estimate reserves an
additional margin for the API and jobs.

## Cost controls

- Make the public demo Terraform default and omit Event Hubs, the producer, and
  the stream processor unless `streaming_enabled` is explicitly set.
- Keep the agent service and batch jobs at zero replicas or executions while idle.
- When streaming is enabled, keep the processor event-driven by default and
  require a separate explicit setting for one continuously running replica.
- Use Event Hubs Basic over AMQP. Do not enable Kafka endpoint, Capture, Standard,
  Premium, or Dedicated tiers without a demonstrated requirement.
- Use ACR Basic and lifecycle-delete unused images.
- Apply short log retention, trace sampling, and prompt/tool-result redaction.
- Set model token, turn, tool-call, and concurrency budgets.
- Add Azure budget alerts at $25, $50, and $100.
- Make the complete environment disposable through `terraform destroy`.

## Price references

- [Azure Container Apps pricing](https://azure.microsoft.com/pricing/details/container-apps/)
- [Azure Container Apps billing FAQ](https://learn.microsoft.com/azure/container-apps/faq#billing)
- [Azure Event Hubs pricing](https://azure.microsoft.com/pricing/details/event-hubs/)
- [Azure Container Registry pricing](https://azure.microsoft.com/pricing/details/container-registry/)
- [Azure Data Lake Storage pricing](https://azure.microsoft.com/pricing/details/storage/data-lake/)
- [Azure Monitor pricing](https://azure.microsoft.com/pricing/details/monitor/)
- [Azure OpenAI pricing](https://azure.microsoft.com/pricing/details/azure-openai/)
