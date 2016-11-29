library(ggplot2)
library(tools)

p95 <- function(values) {
  quantile(values, .95)
}

p99 <- function(values) {
  quantile(values, .99)
}

git_dirty <- (length(system('git status --untracked-files=no --porcelain', intern=TRUE)) > 0)
git_commit <- system('git log -1 --format="%H"', intern=TRUE)

data_mono  <- read.csv('results-ar-monolith.csv')
data_micro <- read.csv('results-ar.csv')

# Make method name friendlier (final plot will feature the same order)
friendly_names  = c("cfs", "bvt", "tie"    , "ps", "fcfs", "tt05"    , "tt20"    , "ttP" , "ttlas")
simulator_names = c("cfs", "bvt", "bvt+tie", "ps", "fifo", "tt_0.005", "tt_0.020", "tt+p", "ttlas")
levels(data_mono$method) <- friendly_names[match(levels(data_mono$method), simulator_names)]
data_mono$method  <- factor(data_mono$method , levels = friendly_names)
levels(data_micro$method) <- friendly_names[match(levels(data_micro$method), simulator_names)]
data_micro$method <- factor(data_micro$method, levels = friendly_names)

# TODO: super-inefficient
data <- NULL
for (method in c('cfs', 'bvt', 'fcfs', 'ttP')) {
  for (app in c("monolithic", "micro-service-based")) {
    load = 0.95;
    if (app == 'monolithic') {
      rt99 = quantile(data_mono$response_time[data_mono$load==load & data_mono$method==method], .99)
    }
    else {
      rt99 = quantile(data_micro$response_time[data_micro$load==load & data_micro$method==method], .99)
    }
    rbind(data, data.frame(load=load, app=app, method=method, rt99=rt99)) -> data
  }
}

x_label = "Scheduling algorithm"
plot_title = sprintf('Simulation Results (%s%s)', git_commit, if (git_dirty) '+' else '')

p <- ggplot(data, aes(x=method, y=rt99, fill=app)) +
  labs(title=plot_title, x=x_label, y = "99th percentile\nresponse time [s]", fill="") +
  geom_bar(stat = "identity", position="dodge") +
  theme(
    axis.title=element_text(size=10),
    legend.key.size = unit(3, "mm"),
    legend.margin = unit(0, "null"),
    legend.position = 'top',
    panel.margin = unit(0,"null"),
    plot.margin = rep(unit(0,"null"),4)
  )

scale=1 # 1.75 for presentations
ggsave('results-mono-vs-micro.pdf', plot=p, height=2.8*scale, width=5*scale, device=cairo_pdf)
