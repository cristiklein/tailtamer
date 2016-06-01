git_dirty <- (length(system('git status --untracked-files=no --porcelain', intern=TRUE)) > 0)
git_commit <- system('git log -1 --format="%H"', intern=TRUE)

data <- read.csv('results.csv')
data$arrival_rate <- as.factor(data$arrival_rate)

plot_title = sprintf('Simulation Results (%s%s)', git_commit, if (git_dirty) '+' else '')

library(ggplot2)
dodge <- position_dodge(width = 0.7)
p <- ggplot(data, aes(x=arrival_rate, y=response_time, fill=method)) +
  geom_violin(position = dodge) +
  labs(title=plot_title, x="Arrival rate [requests/s]", y = "Response time [s]") +
  geom_point(stat = "summary", fun.y = "mean", color = "black", position = dodge)
print(p)

