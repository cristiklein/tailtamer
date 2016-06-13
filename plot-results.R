library(ggplot2)

p95 <- function(values) {
  quantile(values, .95)
}

p99 <- function(values) {
  quantile(values, .99)
}

my_plot <- function(input_file_name, x_column, x_label, x_mult=1) {
  pdf(paste0(input_file_name, ".pdf"), paper='a4r')
  
  git_dirty <- (length(system('git status --untracked-files=no --porcelain', intern=TRUE)) > 0)
  git_commit <- system('git log -1 --format="%H"', intern=TRUE)
  
  data <- read.csv(input_file_name)
  data$x <- as.factor(data[[x_column]]*x_mult)
  
  plot_title = sprintf('Simulation Results (%s%s)', git_commit, if (git_dirty) '+' else '')
  
  dodge <- position_dodge(width = 0.7)
  p <- ggplot(data, aes(x=x, y=response_time, fill=method)) +
    scale_y_continuous(limits=c(0,1), expand = c(0, 0)) +
    geom_violin(position = dodge) +
    labs(title=plot_title, x=x_label, y = "Response time [s]") +
    geom_point(stat = "summary", fun.y = "mean", position = dodge, size = 3, show.legend = FALSE) +
    geom_point(stat = "summary", fun.y = "p95" , position = dodge, size = 2, show.legend = FALSE) +
    geom_point(stat = "summary", fun.y = "p99" , position = dodge, show.legend = FALSE) +
    theme(aspect.ratio=4/3)
  
  print(p)
  dev.off()
}

my_plot('results-ar.csv', 'arrival_rate', 'Arrival rate [req/s]')
my_plot('results-deg.csv', 'degree', 'Call-in degree of last layer')
my_plot('results-mul.csv', 'multiplicity', 'Multiplicity of last layer')
my_plot('results-var.csv', 'relative_variance', 'Relative Variance [%]', x_mult=100)
