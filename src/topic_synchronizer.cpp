#include <memory>
#include <string>
#include <vector>
#include <map>
#include <chrono>
#include <deque>

#include "rclcpp/rclcpp.hpp"
#include "sensor_msgs/msg/point_cloud2.hpp"
#include "sensor_msgs/msg/imu.hpp"
#include "visualization_msgs/msg/marker.hpp"
#include "nav_msgs/msg/odometry.hpp"

class TopicHandlerBase {
public:
    virtual ~TopicHandlerBase() = default;
};

template<typename MsgT>
class TypedTopicHandler : public TopicHandlerBase {
public:
    TypedTopicHandler(
        rclcpp::Node* node,
        const std::string& topic_name, 
        const std::string& mode,
        std::shared_ptr<rclcpp::Time> start_time_ref,
        std::shared_ptr<bool> start_time_set_ref,
        int downsampling_factor,
        int retarder)
    : node_(node), mode_(mode), start_time_(start_time_ref), start_time_set_(start_time_set_ref), downsampling_factor_(downsampling_factor), retarder(retarder), msg_counter_(0)
    {
    
        sub_ = node_->create_subscription<MsgT>(
            topic_name, 
            rclcpp::SensorDataQoS(), 
            std::bind(&TypedTopicHandler::callback, this, std::placeholders::_1));
        
        std::string out_topic = "/synced" + topic_name;
        pub_ = node_->create_publisher<MsgT>(out_topic, 10);
        
        if (retarder > 0) {
            timer_ = node_->create_wall_timer(
                std::chrono::milliseconds(10),
                std::bind(&TypedTopicHandler::timer_callback, this));
        }

        RCLCPP_INFO(node_->get_logger(), "Bridge creato: %s -> %s (Downsampling: %d, Ritardo: %d ms %s)", 
            topic_name.c_str(), out_topic.c_str(), downsampling_factor_, retarder, 
            retarder > 0 ? "con bufferizzazione" : "");
    }

protected:
    void callback(std::shared_ptr<MsgT> msg) {
        msg_counter_++;
        if ((msg_counter_ % downsampling_factor_) != 0) {
            return; // Skip message
        }

        if (retarder <= 0) {
            publish_msg(msg);
        } else {
            buffer_.push_back({node_->now(), msg});
        }
    }

    void timer_callback() {
        if (buffer_.empty()) return;

        rclcpp::Time now = node_->now();
        while (!buffer_.empty()) {
            auto& front = buffer_.front();
            // Se le classi dei clock non sono identiche, Duration sottrae comunque correttamente 
            // ma usiamo rclcpp::Duration per sicurezza
            if ((now - front.first) >= rclcpp::Duration(std::chrono::milliseconds(retarder))) {
                publish_msg(front.second);
                buffer_.pop_front();
            } else {
                break;
            }
        }
    }

    void publish_msg(std::shared_ptr<MsgT> msg) {
        rclcpp::Time now = node_->now();

        if (mode_ == "now") {
            msg->header.stamp = now;
            msg->header.frame_id = "track";
        } 
        else if (mode_ == "relative") {
            // il primo messaggio in assoluto definisce il tempo zero
            if (!(*start_time_set_)) {
                *start_time_ = now;
                *start_time_set_ = true;
                RCLCPP_INFO(node_->get_logger(), "Tempo zero inizializzato a: %f", start_time_->seconds());
            }

            rclcpp::Duration diff = now - *start_time_;
            msg->header.stamp = rclcpp::Time(diff.nanoseconds());
        }

        pub_->publish(*msg);
    }

    rclcpp::Node* node_;
    std::string mode_;
    
    std::shared_ptr<rclcpp::Time> start_time_;
    std::shared_ptr<bool> start_time_set_;
    
    typename rclcpp::Subscription<MsgT>::SharedPtr sub_;
    typename rclcpp::Publisher<MsgT>::SharedPtr pub_;
    rclcpp::TimerBase::SharedPtr timer_;
    
    int downsampling_factor_;
    int retarder;
    uint64_t msg_counter_;

    std::deque<std::pair<rclcpp::Time, std::shared_ptr<MsgT>>> buffer_;

    bool xsens_ = false;
    float min_, max_;
};

class TopicSynchronizer : public rclcpp::Node {
public:
    TopicSynchronizer() : Node("topic_synchronizer") {
        RCLCPP_INFO(this->get_logger(), "Inizializzazione nodo TopicSynchronizer...");

        this->declare_parameter("timestamp_mode", "now");
        this->declare_parameter("topic_names", std::vector<std::string>{});
        this->declare_parameter("topic_types", std::vector<std::string>{});
        this->declare_parameter("downsampling_factors", std::vector<int64_t>{});
        this->declare_parameter("retarder", std::vector<int64_t>{});

        std::string mode = this->get_parameter("timestamp_mode").as_string();
        std::vector<std::string> names = this->get_parameter("topic_names").as_string_array();
        std::vector<std::string> types = this->get_parameter("topic_types").as_string_array();
        std::vector<int64_t> downsampling = this->get_parameter("downsampling_factors").as_integer_array();
        std::vector<int64_t> retarder = this->get_parameter("retarder").as_integer_array();

        // Se downsampling non è specificato, usa 1 per tutti
        if (downsampling.empty()) {
            downsampling.resize(names.size(), 1);
        }

        if (retarder.empty()) {
            retarder.resize(names.size(), 0);
        }

        for (const auto& ret : retarder) {
            if (ret < 0) {
                RCLCPP_ERROR(this->get_logger(), "Errore: retarder deve essere un array di valori non negativi!");
                return;
            }
        }

        RCLCPP_INFO(this->get_logger(), "Modalità: %s. Trovati %zu topic da configurare.", mode.c_str(), names.size());

        if (names.size() != types.size() || names.size() != downsampling.size() || names.size() != retarder.size()) {
            RCLCPP_ERROR(this->get_logger(), "Errore: topic_names, topic_types, downsampling_factors e retarder devono avere la stessa dimensione!");
            return;
        }

        global_start_time_ = std::make_shared<rclcpp::Time>(0);
        global_start_time_set_ = std::make_shared<bool>(false);

        for (size_t i = 0; i < names.size(); ++i) {
            std::string name = names[i];
            std::string type = types[i];
            int factor = static_cast<int>(downsampling[i]);
            int ret = static_cast<int>(retarder[i]);

            create_handler(name, type, mode, factor, ret);
        }
    }

private:
    void create_handler(const std::string& name, const std::string& type, const std::string& mode, int factor, int ret) {
        std::shared_ptr<TopicHandlerBase> handler;

        if (type == "pcl") {
            RCLCPP_INFO(this->get_logger(), "Creazione handler per topic '%s' di tipo PointCloud2 con fattore di downsampling %d e ritardo %d ms", name.c_str(), factor, ret);
            handler = std::make_shared<TypedTopicHandler<sensor_msgs::msg::PointCloud2>>(
                this, name, mode, global_start_time_, global_start_time_set_, factor, ret);
        } 
        else if (type == "imu") {
                RCLCPP_INFO(this->get_logger(), "Creazione handler per topic '%s' di tipo Imu con fattore di downsampling %d e ritardo %d ms", name.c_str(), factor, ret);
            handler = std::make_shared<TypedTopicHandler<sensor_msgs::msg::Imu>>(
                this, name, mode, global_start_time_, global_start_time_set_, factor, ret);
        }
        else if (type == "marker") {
            RCLCPP_INFO(this->get_logger(), "Creazione handler per topic '%s' di tipo Marker con fattore di downsampling %d e ritardo %d ms", name.c_str(), factor, ret);
            handler = std::make_shared<TypedTopicHandler<visualization_msgs::msg::Marker>>(
                this, name, mode, global_start_time_, global_start_time_set_, factor, ret);
        }
        else if (type == "odom") {
            RCLCPP_INFO(this->get_logger(), "Creazione handler per topic '%s' di tipo Odometry con fattore di downsampling %d e ritardo %d ms", name.c_str(), factor, ret);
            handler = std::make_shared<TypedTopicHandler<nav_msgs::msg::Odometry>>(
                this, name, mode, global_start_time_, global_start_time_set_, factor, ret);
        }
        else {
             RCLCPP_WARN(this->get_logger(), "Tipo sconosciuto '%s' per il topic '%s'. Ignorato.", type.c_str(), name.c_str());
             return;
        }

        handlers_.push_back(handler);
    }

    std::vector<std::shared_ptr<TopicHandlerBase>> handlers_;
    
    std::shared_ptr<rclcpp::Time> global_start_time_;
    std::shared_ptr<bool> global_start_time_set_;
};

int main(int argc, char ** argv)
{
  rclcpp::init(argc, argv);
  rclcpp::spin(std::make_shared<TopicSynchronizer>());
  rclcpp::shutdown();
  return 0;
}
