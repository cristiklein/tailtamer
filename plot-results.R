data <- read.csv('results.csv')

methods <- unique(data$method)

library(ggplot2)
p <- ggplot(data, aes(x=method, y=response_time, fill=method)) +
  geom_violin() +
  geom_boxplot(width=0.1, outlier.shape = NA) +
  labs(title="Simulation Results",x="Scheduling Method", y = "Response Times (s)") +
  theme(legend.position = "none")
print(p)