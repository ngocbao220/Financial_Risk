import streamlit as st
import requests
import time

# --- CẤU HÌNH KẾT NỐI ---
# Lưu ý: Nếu chạy trong Docker, hostname thường là tên service trong docker-compose (ví dụ: 'api').
# Nếu chạy local (2 terminal riêng biệt), dùng 'localhost'.
API_BASE_URL = "http://api:8000" 

# --- QUẢN LÝ TRẠNG THÁI (SESSION STATE) ---
# Kiểm tra xem người dùng đã đăng nhập chưa
if 'user_info' not in st.session_state:
    st.session_state['user_info'] = None

# --- HÀM GỌI API ---
def api_get_all_users():
    try:
        response = requests.get(f"{API_BASE_URL}/user/get_all")
        if response.status_code == 200:
            return response.json()
        return []
    except:
        return []

# --- HÀM GỌI API ---
def api_login(username):
    """Gọi API tạo user để 'đăng nhập'"""
    url = f"{API_BASE_URL}/user/create"
    payload = {"username": username}
    
    try:
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code == 200:
            return response.json() # Trả về dict user info từ Redis
        else:
            st.error(f"Lỗi API ({response.status_code}): {response.text}")
            return None
    except requests.exceptions.ConnectionError:
        st.error("🔴 Không thể kết nối tới API. Hãy kiểm tra xem Docker container 'api' có đang chạy không.")
        return None

def api_get_balance(user_id):
    """Gọi API lấy thông tin mới nhất của user"""
    url = f"{API_BASE_URL}/user/get/{user_id}"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

# --- GIAO DIỆN: TRANG ĐĂNG NHẬP ---
def show_login():
    st.set_page_config(page_title="Crypto Login", layout="centered")
    st.title("🔐 Sàn Giao Dịch Giả Lập")
    st.markdown("---")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.info("Hệ thống sử dụng Redis để cấp ID và ví mới cho mỗi lần nhập tên.")
        
        with st.form("login_form"):
            username = st.text_input("Tên Trader:", placeholder="Nhập nickname")
            submitted = st.form_submit_button("🚀 Truy cập hệ thống", use_container_width=True)
            
            if submitted:
                if not username.strip():
                    st.warning("Vui lòng nhập tên!")
                else:
                    with st.spinner("Đang khởi tạo ví trên Blockchain (Redis)..."):
                        user_data = api_login(username)
                        
                    if user_data:
                        # Đăng nhập thành công -> Lưu vào session
                        st.session_state['user_info'] = user_data
                        st.success("Đăng nhập thành công!")
                        time.sleep(0.5)
                        st.rerun()

def show_dashboard():
    user = st.session_state['user_info']
    
    # Sidebar thông tin
    with st.sidebar:
        st.header(f"👤 {user['username']}")
        st.caption(f"ID: {user['user_id']}")
        st.divider()
        
        # Hiển thị số dư
        st.metric("Số dư USD", f"${user['usd']:,.2f}")
        st.metric("Số dư BTC", f"{user['btc']:.6f} BTC")
        
        st.divider()
        if st.button("Đăng xuất / Reset"):
            st.session_state['user_info'] = None
            st.rerun()

    # Màn hình chính
    st.title("📈 Dashboard Giao Dịch")
    
    # Tab chức năng
    tab1, tab2, tab3 = st.tabs(["Giao dịch", "Lịch sử", "Danh sách User (Admin)"])
    
    with tab1:
        col_trade_1, col_trade_2 = st.columns(2)
        with col_trade_1:
            st.subheader("Đặt lệnh Mua/Bán")
            # Form đặt lệnh (Sẽ kết nối API trade sau)
            trade_type = st.radio("Loại lệnh", ["MUA (Buy)", "BÁN (Sell)"], horizontal=True)
            amount = st.number_input("Số lượng (USD hoặc BTC)", min_value=0.0)
            if st.button("Gửi lệnh", use_container_width=True):
                st.toast(f"Đang gửi lệnh {trade_type} - Chức năng đang phát triển...")
        
        with col_trade_2:
            st.subheader("Thị trường")
            st.info("Biểu đồ nến sẽ hiển thị ở đây")

    with tab2:
        st.write("Chưa có lịch sử giao dịch.")

    with tab3:
        st.subheader("👥 Danh sách người dùng trong hệ thống")
        if st.button("Làm mới danh sách"):
            st.rerun()
            
        all_users = api_get_all_users()
        
        if all_users:
            # --- ĐOẠN CODE SỬA LỖI ---
            # Kiểm tra xem dữ liệu trả về có phải là Dict không (nguyên nhân gây lỗi)
            if isinstance(all_users, dict):
                # Nếu API trả về lỗi (thường FastAPI trả về key 'detail' khi lỗi)
                if "detail" in all_users:
                    st.error(f"Lỗi từ API: {all_users['detail']}")
                    st.stop() # Dừng lại không vẽ bảng nữa
                
                # Nếu không phải lỗi mà là 1 user lẻ, bọc nó vào list
                all_users = [all_users]
            # -------------------------

            # Chuyển thành DataFrame
            import pandas as pd
            df = pd.DataFrame(all_users)
            
            # Kiểm tra xem DataFrame có dữ liệu không trước khi gán cột
            if not df.empty:
                # Chỉ đổi tên cột nếu số lượng cột khớp (tránh lỗi lệch cột)
                if len(df.columns) == 4:
                    df.columns = ["User ID", "Tên", "Số dư USD", "Số dư BTC"]
                
                st.dataframe(df, use_container_width=True)
            else:
                st.warning("Dữ liệu trả về rỗng.")
        else:
            st.info("Chưa có người dùng nào khác.")

# --- HÀM MAIN ĐIỀU HƯỚNG ---
def main():
    if st.session_state['user_info']:
        # Nếu có thông tin trong session -> Hiện Dashboard
        # (Optional) Refresh data user mỗi lần reload để số dư chính xác
        # refreshed_user = api_get_balance(st.session_state['user_info']['user_id'])
        # if refreshed_user: st.session_state['user_info'] = refreshed_user
        
        show_dashboard()
    else:
        # Nếu chưa có -> Hiện trang Login
        show_login()

if __name__ == "__main__":
    main()