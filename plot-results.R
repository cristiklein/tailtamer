data <- read.csv('results.csv')
data$arrival_rate <- as.factor(data$arrival_rate)

library(ggplot2)
dodge <- position_dodge(width = 0.7)
p <- ggplot(data, aes(x=arrival_rate, y=response_time, fill=method)) +
  geom_violin(position = dodge) +
  labs(title="Simulation Results", x="Arrival rate [requests/s]", y = "Response time [s]") +
  geom_point(stat = "summary", fun.y = "mean", color = "black", position = dodge)
print(p)