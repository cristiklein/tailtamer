git_dirty <- (length(system('git status --untracked-files=no --porcelain', intern=TRUE)) > 0)
git_commit <- system('git log -1 --format="%H"', intern=TRUE)

data <- read.csv('results-var.csv')
data$relative_variance <- as.factor(data$relative_variance*100)

plot_title = sprintf('Simulation Results (%s%s)', git_commit, if (git_dirty) '+' else '')

p95 <- function(values) {
  quantile(values, .95)
}

p99 <- function(values) {
  quantile(values, .99)
}

library(ggplot2)
dodge <- position_dodge(width = 0.7)
p <- ggplot(data, aes(x=relative_variance, y=response_time, fill=method)) +
  scale_y_continuous(limits=c(0,1), expand = c(0, 0)) +
  geom_violin(position = dodge) +
  labs(title=plot_title, x="Relative variance [%]", y = "Response time [s]") +
  geom_point(stat = "summary", fun.y = "mean", position = dodge, size = 3, show.legend = FALSE) +
  geom_point(stat = "summary", fun.y = "p95" , position = dodge, size = 2, show.legend = FALSE) +
  geom_point(stat = "summary", fun.y = "p99" , position = dodge, show.legend = FALSE)
print(p)